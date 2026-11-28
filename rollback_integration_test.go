package main

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestSequencerPixelRollbackIntegration(t *testing.T) {
	rdb, canvas, eventLog, rollback := newTestRollback(t)
	ctx := context.Background()

	resolver, err := NewConflictResolver("sequencer", rdb, "")
	if err != nil {
		t.Fatalf("NewConflictResolver: %v", err)
	}

	// Create a checkpoint before the pixel event.
	if err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: eventStreamKey,
		Values: map[string]interface{}{
			"type": "checkpoint",
		},
	}).Err(); err != nil {
		t.Fatalf("create checkpoint: %v", err)
	}

	checkpoints, err := eventLog.Range(ctx, "-", "+", 1)
	if err != nil {
		t.Fatalf("read checkpoint: %v", err)
	}
	targetID := checkpoints[0].ID

	// Write a red pixel.
	_, err = resolver.Apply(ctx, PixelEvent{
		X:         10,
		Y:         10,
		Color:     "#ff0000",
		UserID:    "integration-test",
		Timestamp: 123456789,
	})
	if err != nil {
		t.Fatalf("Apply pixel: %v", err)
	}

	// Verify the pixel is red.
	data, err := canvas.GetChunk(ctx, ChunkCoord{CX: 0, CY: 0})
	if err != nil {
		t.Fatalf("GetChunk before rollback: %v", err)
	}

	_, offset := locate(10, 10)
	if got := data[offset : offset+3]; string(got) != string([]byte{255, 0, 0}) {
		t.Fatalf("pixel before rollback = %v; want red", got)
	}

	// Roll back to the checkpoint.
	if err := rollback.Rollback(ctx, targetID); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// Verify the pixel is white again.
	data, err = canvas.GetChunk(ctx, ChunkCoord{CX: 0, CY: 0})
	if err != nil {
		t.Fatalf("GetChunk after rollback: %v", err)
	}

	if got := data[offset : offset+3]; string(got) != string([]byte{255, 255, 255}) {
		t.Fatalf("pixel after rollback = %v; want white", got)
	}
}
