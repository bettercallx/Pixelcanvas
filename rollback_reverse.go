package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ReverseUndoRollback restores events newer than targetID in newest-first
// order, using PrevColor for pixel events. targetID is inclusive: the canvas
// is left in the state immediately after that event.
//
// TODO: handle writes while Rollback runs, publish WebSocket updates or append compensating events to the stream.
type ReverseUndoRollback struct {
	canvas *Canvas
	log    *EventLog
}

var publishRollbackVersionScript = redis.NewScript(`
-- KEYS: active version, event stream, rollback lock
-- ARGV: expected active version, staging version, lock token, target ID

local lockToken = redis.call('GET', KEYS[3])
if lockToken ~= ARGV[3] then
  return redis.error_reply('rollback lock is no longer owned')
end

local active = redis.call('GET', KEYS[1])
if active ~= ARGV[1] then
  return redis.error_reply('active canvas version changed during rollback')
end

local stagingChunksKey = 'canvas:v' .. ARGV[2] .. ':chunks'
if redis.call('EXISTS', stagingChunksKey) == 0 then
  return redis.error_reply('staging canvas version is missing')
end

-- Publish the completed version and record the rollback atomically.
redis.call('SET', KEYS[1], ARGV[2])
redis.call('XADD', KEYS[2], '*',
  'type', 'rollback',
  'target_id', ARGV[4])

return 1
`)

var discardCanvasVersionScript = redis.NewScript(`
-- ARGV: version to discard

local version = ARGV[1]
local active = redis.call('GET', KEYS[1])

if active == version then
  return redis.error_reply('refusing to discard the active canvas version')
end

local chunksKey = 'canvas:v' .. version .. ':chunks'
local members = redis.call('SMEMBERS', chunksKey)

for _, member in ipairs(members) do
  redis.call('DEL',
    'canvas:v' .. version .. ':chunk:' .. member,
    'canvas:v' .. version .. ':clock:' .. member)
end

redis.call('DEL', chunksKey)
return 1
`)

func NewReverseUndoRollback(canvas *Canvas, log *EventLog) *ReverseUndoRollback {
	return &ReverseUndoRollback{canvas: canvas, log: log}
}

func (r *ReverseUndoRollback) Name() string { return "reverse" }

func (r *ReverseUndoRollback) Rollback(ctx context.Context, targetID string) (retErr error) {
	if r == nil || r.canvas == nil || r.log == nil {
		return fmt.Errorf("reverse rollback is not fully configured")
	}

	lock, err := AcquireRollbackLock(ctx, r.canvas.rdb, 10*time.Second)
	if err != nil {
		return err
	}

	workCtx, stopKeepAlive := lock.KeepAlive(ctx, 3*time.Second)

	stagingVersion := ""
	published := false

	defer func() {
		// If publication did not succeed, discard the staging version.
		if stagingVersion != "" && !published {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			_, cleanupErr := discardCanvasVersionScript.Run(
				cleanupCtx,
				r.canvas.rdb,
				[]string{activeVersionKey},
				stagingVersion,
			).Result()
			cancel()

			if cleanupErr != nil && retErr == nil {
				retErr = fmt.Errorf("discard staging canvas version %q: %w", stagingVersion, cleanupErr)
			}
		}

		if keepAliveErr := stopKeepAlive(); keepAliveErr != nil && retErr == nil {
			retErr = fmt.Errorf("rollback lock renewal failed: %w", keepAliveErr)
		}

		releaseCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		if releaseErr := lock.Release(releaseCtx); releaseErr != nil && retErr == nil {
			retErr = fmt.Errorf("release rollback lock: %w", releaseErr)
		}
	}()

	ctx = workCtx

	if err := validateRollbackTarget(ctx, r.log, targetID); err != nil {
		return err
	}

	// Remember the version that was active when this rollback began.
	sourceVersion, err := activeVersion(ctx, r.canvas.rdb)
	if err != nil {
		return fmt.Errorf("get active canvas version: %w", err)
	}

	// Create an isolated staging copy.
	stagingVersion, err = cloneActiveCanvasVersion(ctx, r.canvas.rdb)
	if err != nil {
		return fmt.Errorf("clone active canvas version: %w", err)
	}

	// Read all events after the target, newest first.
	events, err := r.log.RevRange(ctx, "+", "("+targetID, 0)
	if err != nil {
		return fmt.Errorf("read events after rollback target %q: %w", targetID, err)
	}

	// Apply undo operations exclusively to the staging version.
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return err
		}

		switch event.Type {
		case "pixel":
			if err := r.undoPixel(ctx, stagingVersion, event); err != nil {
				return fmt.Errorf(
					"undo pixel event %s at (%d,%d): %w",
					event.ID, event.X, event.Y, err,
				)
			}

		case "chunk_added":
			if err := r.undoChunkAdded(ctx, stagingVersion, event); err != nil {
				return fmt.Errorf(
					"undo chunk_added event %s for chunk (%d,%d): %w",
					event.ID, event.CX, event.CY, err,
				)
			}

		default:
			return fmt.Errorf(
				"event %s has type %q: %w",
				event.ID, event.Type, ErrUnsupportedEvent,
			)
		}
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	// Atomically publish the staging version and append the rollback marker.
	_, err = publishRollbackVersionScript.Run(
		ctx,
		r.canvas.rdb,
		[]string{activeVersionKey, eventStreamKey, rollbackLockKey},
		sourceVersion,
		stagingVersion,
		lock.token,
		targetID,
	).Result()
	if err != nil {
		return fmt.Errorf("publish rollback version: %w", err)
	}

	published = true
	return nil
}

func (r *ReverseUndoRollback) undoPixel(
	ctx context.Context,
	version string,
	event LoggedEvent,
) error {
	rgb, err := parseHexColor(event.PrevColor)
	if err != nil {
		return fmt.Errorf("invalid prev_color %q: %w", event.PrevColor, err)
	}

	cc, offset := locate(event.X, event.Y)
	chunkKey := versionChunkKey(version, cc)

	_, err = r.canvas.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.SetRange(ctx, chunkKey, offset, string(rgb[:]))
		return nil
	})
	return err
}

func (r *ReverseUndoRollback) undoChunkAdded(
	ctx context.Context,
	version string,
	event LoggedEvent,
) error {
	cc := ChunkCoord{CX: event.CX, CY: event.CY}

	chunkKey := versionChunkKey(version, cc)
	clockKey := versionClockKey(version, cc)
	chunksKey := versionChunksKey(version)

	_, err := r.canvas.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Del(ctx, chunkKey, clockKey)
		p.SRem(ctx, chunksKey, cc.member())
		return nil
	})
	return err
}
