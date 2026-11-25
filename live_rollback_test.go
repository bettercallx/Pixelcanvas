package main

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestLiveRedisRollbackSetup(t *testing.T) {
	ctx := context.Background()

	// Connect to the same Redis instance as the running server.
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis connection: %v", err)
	}

	canvas, err := NewCanvas(ctx, rdb, 0)
	if err != nil {
		t.Fatalf("NewCanvas: %v", err)
	}

	eventLog := NewEventLog(rdb)
	rollback := &ReverseUndoRollback{
		canvas: canvas,
		log:    eventLog,
	}

	resolver, err := NewConflictResolver("sequencer", rdb, "")
	if err != nil {
		t.Fatalf("NewConflictResolver: %v", err)
	}

	// Create a checkpoint.
	checkpoint, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: eventStreamKey,
		Values: map[string]interface{}{
			"type": "checkpoint",
		},
	}).Result()
	if err != nil {
		t.Fatalf("create checkpoint: %v", err)
	}

	t.Logf("Checkpoint ID: %s", checkpoint)

	// Write a red pixel.
	_, err = resolver.Apply(ctx, PixelEvent{
		X:         10,
		Y:         10,
		Color:     "#ff0000",
		UserID:    "live-integration-test",
		Timestamp: 123456789,
	})
	if err != nil {
		t.Fatalf("Apply pixel: %v", err)
	}

	// Confirm the pixel write.
	data, err := canvas.GetChunk(ctx, ChunkCoord{CX: 0, CY: 0})
	if err != nil {
		t.Fatalf("GetChunk: %v", err)
	}

	_, offset := locate(10, 10)
	if got := data[offset : offset+3]; string(got) != string([]byte{255, 0, 0}) {
		t.Fatalf("pixel = %v; want red", got)
	}

	t.Log("Pixel successfully written. Rolling back...")

	if err := rollback.Rollback(ctx, checkpoint); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Confirm rollback restored the pixel.
	data, err = canvas.GetChunk(ctx, ChunkCoord{CX: 0, CY: 0})
	if err != nil {
		t.Fatalf("GetChunk after rollback: %v", err)
	}

	if got := data[offset : offset+3]; string(got) != string([]byte{255, 255, 255}) {
		t.Fatalf("pixel after rollback = %v; want white", got)
	}

	t.Log("Live Redis rollback succeeded.")
}

func TestLiveRedisCreatePixelForHTTPRollback(t *testing.T) {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Fatalf("Redis connection: %v", err)
	}

	resolver, err := NewConflictResolver("sequencer", rdb, "")
	if err != nil {
		t.Fatalf("NewConflictResolver: %v", err)
	}

	_, err = resolver.Apply(ctx, PixelEvent{
		X:         20,
		Y:         20,
		Color:     "#0000ff",
		UserID:    "http-rollback-test",
		Timestamp: 123456790,
	})
	if err != nil {
		t.Fatalf("Apply pixel: %v", err)
	}

	t.Log("Successfully changed pixel (20, 20) to blue.")
}
