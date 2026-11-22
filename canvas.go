// an infinite canvas split into 64x64 chunks stored in Redis
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
)

const chunksSetKey = "canvas:chunks" // Redis Set of opened chunks, members like "0:0", "-1:2"

var (
	ErrChunkNotOpen = errors.New("chunk is not open")
	ErrMaxChunks    = errors.New("canvas reached max chunks")
)

// blank chunk content: every pixel white
var whiteChunk = bytes.Repeat([]byte{0xff}, chunkBytes)

// canvas state in Redis
type Canvas struct {
	rdb       *redis.Client
	maxChunks int
}

// openChunk queues "fill the chunk white (only if it has no data yet) and mark it open" on p.
// Callers run it inside TxPipelined so other servers never see one step without the other.
func openChunk(ctx context.Context, p redis.Pipeliner, cc ChunkCoord) {
	p.SetNX(ctx, cc.key(), whiteChunk, 0)
	p.SAdd(ctx, chunksSetKey, cc.member())
}

// NewCanvas opens chunk (0,0) on first start
func NewCanvas(ctx context.Context, rdb *redis.Client, maxChunks int) (*Canvas, error) {
	c := &Canvas{rdb: rdb, maxChunks: maxChunks}
	_, err := rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		openChunk(ctx, p, ChunkCoord{0, 0})
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("open origin chunk: %w", err)
	}
	return c, nil
}

func (c *Canvas) IsOpen(ctx context.Context, cc ChunkCoord) (bool, error) {
	return c.rdb.SIsMember(ctx, chunksSetKey, cc.member()).Result()
}

// setPixelScript checks the chunk is open, reads the old color, writes the new one and logs to the event log.
// Redis runs a whole script atomically.
var setPixelScript = redis.NewScript(`
-- KEYS[1] = canvas:chunks       set of open chunks
-- KEYS[2] = canvas:chunk:cx:cy  the pixel's chunk
-- KEYS[3] = canvas:events       event log stream
-- ARGV[1] = chunk member "cx:cy"
-- ARGV[2] = byte offset of the pixel inside the chunk
-- ARGV[3] = new color as 3 raw bytes
-- ARGV[4..7] = x, y, user_id, timestamp (only for the log)

-- 3 raw bytes -> "#rrggbb" (always lowercase, so colors in the log compare reliably)
local function hex(rgb)
  return string.format('#%02x%02x%02x', string.byte(rgb, 1, 3))
end

if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  return false  -- arrives in Go as redis.Nil
end

local offset = tonumber(ARGV[2])
local old = redis.call('GETRANGE', KEYS[2], offset, offset + 2)
redis.call('SETRANGE', KEYS[2], offset, ARGV[3])
redis.call('XADD', KEYS[3], '*',
  'type', 'pixel',
  'x', ARGV[4], 'y', ARGV[5],
  'color', hex(ARGV[3]),
  'prev_color', hex(old),
  'user_id', ARGV[6],
  'timestamp', ARGV[7])
return 1
`)

// SetPixel writes one pixel and logs it with its previous color
func (c *Canvas) SetPixel(ctx context.Context, ev PixelEvent) error {
	rgb, err := parseHexColor(ev.Color)
	if err != nil {
		return err
	}
	cc, offset := locate(ev.X, ev.Y)

	keys := []string{chunksSetKey, cc.key(), eventStreamKey}
	err = setPixelScript.Run(ctx, c.rdb, keys,
		cc.member(), offset, string(rgb[:]),
		ev.X, ev.Y, ev.UserID, ev.Timestamp,
	).Err()
	if errors.Is(err, redis.Nil) {
		return ErrChunkNotOpen
	}
	return err
}

// GetChunk returns the RGB bytes of one chunk
func (c *Canvas) GetChunk(ctx context.Context, cc ChunkCoord) ([]byte, error) {
	data, err := c.rdb.Get(ctx, cc.key()).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrChunkNotOpen
	}
	return data, err
}

// AddChunk opens a new chunk. Returns false if it was already open.
// before calling AddChunk check rules: adjacent/free
func (c *Canvas) AddChunk(ctx context.Context, cc ChunkCoord) (bool, error) {
	open, err := c.IsOpen(ctx, cc)
	if err != nil || open {
		return false, err
	}
	// soft cap: two concurrent adds can overshoot by a chunk or two, which is harmless
	n, err := c.rdb.SCard(ctx, chunksSetKey).Result()
	if err != nil {
		return false, err
	}
	if int(n) >= c.maxChunks {
		return false, ErrMaxChunks
	}
	// open + log in one transaction
	_, err = c.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		openChunk(ctx, p, cc)
		appendChunkEvent(ctx, p, cc)
		return nil
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

func (c *Canvas) ListChunks(ctx context.Context) ([]ChunkCoord, error) {
	members, err := c.rdb.SMembers(ctx, chunksSetKey).Result()
	if err != nil {
		return nil, err
	}
	coords := make([]ChunkCoord, 0, len(members))
	for _, m := range members {
		var cc ChunkCoord
		if _, err := fmt.Sscanf(m, "%d:%d", &cc.CX, &cc.CY); err != nil {
			return nil, fmt.Errorf("bad chunk member %q: %w", m, err)
		}
		coords = append(coords, cc)
	}
	return coords, nil
}
