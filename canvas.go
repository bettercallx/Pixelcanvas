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

// GetChunk returns the RGB bytes of one chunk
func (c *Canvas) GetChunk(ctx context.Context, cc ChunkCoord) ([]byte, error) {
	data, err := c.rdb.Get(ctx, cc.key()).Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, ErrChunkNotOpen
	}
	return data, err
}

// GetChunkWithSeq also returns the global seq at read time; events with seq <= it are already in the bytes
func (c *Canvas) GetChunkWithSeq(ctx context.Context, cc ChunkCoord) ([]byte, int64, error) {
	var get *redis.StringCmd
	var seq *redis.StringCmd
	_, err := c.rdb.TxPipelined(ctx, func(p redis.Pipeliner) error {
		get = p.Get(ctx, cc.key())
		seq = p.Get(ctx, seqKey)
		return nil
	})
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, 0, err
	}
	data, err := get.Bytes()
	if errors.Is(err, redis.Nil) {
		return nil, 0, ErrChunkNotOpen
	}
	if err != nil {
		return nil, 0, err
	}
	n, err := seq.Int64()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, 0, err
	}
	return data, n, nil
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
