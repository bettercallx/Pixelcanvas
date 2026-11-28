// an infinite canvas split into 64x64 chunks stored in Redis
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strconv"

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

	if err := initializeCanvasVersion(ctx, rdb); err != nil {
		return nil, fmt.Errorf("initialize canvas version: %w", err)
	}

	return c, nil
}

func (c *Canvas) IsOpen(ctx context.Context, cc ChunkCoord) (bool, error) {
	return c.rdb.SIsMember(ctx, chunksSetKey, cc.member()).Result()
}

// GetChunk returns the RGB bytes of one chunk
func (c *Canvas) GetChunk(ctx context.Context, cc ChunkCoord) ([]byte, error) {
	data, err := getChunkVersionScript.Run(
		ctx,
		c.rdb,
		[]string{activeVersionKey},
		cc.member(),
	).Text()

	if errors.Is(err, redis.Nil) {
		return nil, ErrChunkNotOpen
	}
	if err != nil {
		return nil, err
	}

	return []byte(data), nil
}

// GetChunkWithSeq also returns the global seq at read time; events with seq <= it are already in the bytes
func (c *Canvas) GetChunkWithSeq(ctx context.Context, cc ChunkCoord) ([]byte, int64, error) {
	res, err := getChunkWithSeqVersionScript.Run(
		ctx,
		c.rdb,
		[]string{activeVersionKey, seqKey},
		cc.member(),
	).Slice()

	if errors.Is(err, redis.Nil) {
		return nil, 0, ErrChunkNotOpen
	}
	if err != nil {
		return nil, 0, err
	}

	if len(res) == 0 || res[0].(int64) == 0 {
		return nil, 0, ErrChunkNotOpen
	}

	data, ok := res[1].(string)
	if !ok {
		return nil, 0, fmt.Errorf("unexpected chunk data type: %T", res[1])
	}

	seq, err := strconv.ParseInt(res[2].(string), 10, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("parse canvas sequence: %w", err)
	}

	return []byte(data), seq, nil
}

// AddChunk opens a new chunk. Returns false if it was already open.
// before calling AddChunk check rules: adjacent/free
func (c *Canvas) AddChunk(ctx context.Context, cc ChunkCoord) (bool, error) {
	added := false

	err := c.rdb.Watch(ctx, func(tx *redis.Tx) error {
		locked, err := tx.Exists(ctx, rollbackLockKey).Result()
		if err != nil {
			return err
		}
		if locked > 0 {
			return ErrRollbackInProgress
		}

		version, err := tx.Get(ctx, activeVersionKey).Result()
		if err != nil {
			return err
		}

		chunksKey := versionChunksKey(version)
		chunkKey := versionChunkKey(version, cc)

		open, err := tx.SIsMember(ctx, chunksKey, cc.member()).Result()
		if err != nil {
			return err
		}
		if open {
			return nil
		}

		n, err := tx.SCard(ctx, chunksKey).Result()
		if err != nil {
			return err
		}
		if int(n) >= c.maxChunks {
			return ErrMaxChunks
		}

		_, err = tx.TxPipelined(ctx, func(p redis.Pipeliner) error {
			p.Set(ctx, chunkKey, whiteChunk, 0)
			p.SAdd(ctx, chunksKey, cc.member())
			appendChunkEvent(ctx, p, cc)
			return nil
		})
		if err != nil {
			return err
		}

		added = true
		return nil
	}, rollbackLockKey, activeVersionKey)

	if err != nil {
		return false, err
	}

	return added, nil
}

func (c *Canvas) ListChunks(ctx context.Context) ([]ChunkCoord, error) {
	version, err := activeVersion(ctx, c.rdb)
	if err != nil {
		return nil, err
	}

	members, err := c.rdb.SMembers(ctx, versionChunksKey(version)).Result()
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
