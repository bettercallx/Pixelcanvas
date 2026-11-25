package main

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func appendTestPixelEvent(t *testing.T, rdb *redis.Client, x, y int, color, prevColor string) string {
	t.Helper()
	id, err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: eventStreamKey,
		Values: map[string]any{
			"type":       "pixel",
			"x":          x,
			"y":          y,
			"color":      color,
			"prev_color": prevColor,
		},
	}).Result()
	if err != nil {
		t.Fatalf("append test pixel event: %v", err)
	}
	return id
}

func appendTestEvent(t *testing.T, rdb *redis.Client, eventType string) string {
	t.Helper()
	id, err := rdb.XAdd(context.Background(), &redis.XAddArgs{
		Stream: eventStreamKey,
		Values: map[string]any{"type": eventType},
	}).Result()
	if err != nil {
		t.Fatalf("append test %q event: %v", eventType, err)
	}
	return id
}

func newTestRollback(t *testing.T) (*redis.Client, *Canvas, *EventLog, *ReverseUndoRollback) {
	t.Helper()
	rdb, canvas := newTestCanvas(t)
	log := NewEventLog(rdb)
	return rdb, canvas, log, NewReverseUndoRollback(canvas, log)
}

func setTestPixel(t *testing.T, rdb *redis.Client, x, y int, color string) {
	t.Helper()

	ctx := context.Background()
	version, err := activeVersion(ctx, rdb)
	if err != nil {
		t.Fatalf("get active version: %v", err)
	}

	cc, offset := locate(x, y)
	key := versionChunkKey(version, cc)

	if err := rdb.SetRange(ctx, key, offset, color).Err(); err != nil {
		t.Fatalf("set test pixel: %v", err)
	}
}

func assertRollbackMarker(t *testing.T, log *EventLog, targetID string) {
	t.Helper()
	events, err := log.Range(context.Background(), "-", "+", 0)
	if err != nil {
		t.Fatalf("read event log: %v", err)
	}
	if len(events) == 0 {
		t.Fatal("event log is empty; expected rollback marker")
	}
	last := events[len(events)-1]
	if last.Type != "rollback" {
		t.Fatalf("last event type = %q, want rollback", last.Type)
	}
	if last.TargetID != targetID {
		t.Errorf("rollback target_id = %q, want %q", last.TargetID, targetID)
	}
}

func TestReverseUndoRollbackRestoresPixel(t *testing.T) {
	rdb, canvas, log, rollback := newTestRollback(t)
	ctx := context.Background()

	// The first event is the rollback boundary.
	targetID := appendTestPixelEvent(t, rdb, 4, 7, "#ff0000", "#ffffff")
	setTestPixel(t, rdb, 4, 7, "\xff\x00\x00")

	// A later event changes the pixel to blue.
	appendTestPixelEvent(t, rdb, 4, 7, "#0000ff", "#ff0000")
	setTestPixel(t, rdb, 4, 7, "\x00\x00\xff")

	// Record the original version and its pixel data.
	originalVersion, err := activeVersion(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}

	if err := rollback.Rollback(ctx, targetID); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	// The rollback must publish a different active version.
	newVersion, err := activeVersion(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}
	if newVersion == originalVersion {
		t.Fatal("active version did not change after rollback")
	}

	// The original version must remain unchanged.
	if got := rdb.Get(
		ctx,
		versionChunkKey(originalVersion, ChunkCoord{CX: 0, CY: 0}),
	).Val(); got == "" {
		t.Fatal("original version chunk is missing")
	}

	// The newly active version must contain the restored pixel.
	if got := pixelColor(t, canvas, 4, 7); got != "#ff0000" {
		t.Errorf("pixel after rollback = %s, want #ff0000", got)
	}

	// The original version should still contain the pre-rollback blue pixel.
	originalData, err := rdb.Get(
		ctx,
		versionChunkKey(originalVersion, ChunkCoord{CX: 0, CY: 0}),
	).Bytes()
	if err != nil {
		t.Fatal(err)
	}

	_, offset := locate(4, 7)
	if got := originalData[offset : offset+3]; string(got) != "\x00\x00\xff" {
		t.Errorf("original version pixel = %v, want blue", got)
	}

	assertRollbackMarker(t, log, targetID)
}

func TestReverseUndoRollbackUndoesRepeatedWritesInReverseOrder(t *testing.T) {
	rdb, canvas, _, rollback := newTestRollback(t)
	ctx := context.Background()

	targetID := appendTestPixelEvent(t, rdb, 2, 3, "#ff0000", "#ffffff")
	setTestPixel(t, rdb, 2, 3, "\xff\x00\x00")

	appendTestPixelEvent(t, rdb, 2, 3, "#00ff00", "#ff0000")
	setTestPixel(t, rdb, 2, 3, "\x00\xff\x00")

	appendTestPixelEvent(t, rdb, 2, 3, "#0000ff", "#00ff00")
	setTestPixel(t, rdb, 2, 3, "\x00\x00\xff")

	if err := rollback.Rollback(ctx, targetID); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	if got := pixelColor(t, canvas, 2, 3); got != "#ff0000" {
		t.Errorf("pixel after rollback = %s, want #ff0000", got)
	}
}

func TestReverseUndoRollbackRestoresMultiplePixels(t *testing.T) {
	rdb, canvas, _, rollback := newTestRollback(t)
	ctx := context.Background()

	targetID := appendTestPixelEvent(t, rdb, 1, 1, "#111111", "#ffffff")
	setTestPixel(t, rdb, 1, 1, "\x11\x11\x11")

	appendTestPixelEvent(t, rdb, 5, 6, "#222222", "#ffffff")
	setTestPixel(t, rdb, 5, 6, "\x22\x22\x22")

	appendTestPixelEvent(t, rdb, 8, 9, "#333333", "#ffffff")
	setTestPixel(t, rdb, 8, 9, "\x33\x33\x33")

	if err := rollback.Rollback(ctx, targetID); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	for _, pixel := range []struct{ x, y int }{{5, 6}, {8, 9}} {
		if got := pixelColor(t, canvas, pixel.x, pixel.y); got != "#ffffff" {
			t.Errorf("pixel (%d,%d) after rollback = %s, want #ffffff", pixel.x, pixel.y, got)
		}
	}

	if got := pixelColor(t, canvas, 1, 1); got != "#111111" {
		t.Errorf("target pixel after rollback = %s, want #111111", got)
	}
}

func TestReverseUndoRollbackRemovesChunkAddedAfterTarget(t *testing.T) {
	rdb, canvas, _, rollback := newTestRollback(t)
	ctx := context.Background()
	targetID := appendTestEvent(t, rdb, "checkpoint")

	cc := ChunkCoord{CX: 1, CY: 0}
	added, err := canvas.AddChunk(ctx, cc)
	if err != nil || !added {
		t.Fatalf("AddChunk = %v, %v; want true, nil", added, err)
	}
	version, err := activeVersion(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}
	if err := rdb.HSet(ctx, versionClockKey(version, cc), "0", "12:server-a").Err(); err != nil {
		t.Fatal(err)
	}

	if err := rollback.Rollback(ctx, targetID); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if open, err := canvas.IsOpen(ctx, cc); err != nil || open {
		t.Errorf("chunk open after rollback = %v, %v; want false, nil", open, err)
	}
	// The old version is retained, but the newly active version must
	// not contain the removed chunk or its clock metadata.
	activeVer, err := activeVersion(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}

	if exists, err := rdb.Exists(
		ctx,
		versionChunkKey(activeVer, cc),
		versionClockKey(activeVer, cc),
	).Result(); err != nil || exists != 0 {
		t.Errorf(
			"chunk and clock keys remaining in active version = %d, %v; want 0, nil",
			exists,
			err,
		)
	}
}

func TestReverseUndoRollbackRejectsInvalidTarget(t *testing.T) {
	_, _, _, rollback := newTestRollback(t)

	err := rollback.Rollback(context.Background(), "9999999999999-0")
	if !errors.Is(err, ErrInvalidRollbackTarget) {
		t.Errorf("Rollback error = %v, want ErrInvalidRollbackTarget", err)
	}
}

func TestReverseUndoRollbackRejectsEmptyTarget(t *testing.T) {
	_, _, _, rollback := newTestRollback(t)

	err := rollback.Rollback(context.Background(), "")
	if !errors.Is(err, ErrInvalidRollbackTarget) {
		t.Errorf("Rollback error = %v, want ErrInvalidRollbackTarget", err)
	}
}

func TestReverseUndoRollbackRejectsUnsupportedEvent(t *testing.T) {
	rdb, _, _, rollback := newTestRollback(t)
	ctx := context.Background()

	targetID := appendTestEvent(t, rdb, "checkpoint")
	appendTestEvent(t, rdb, "unexpected_type")

	// Capture the original active version and its data.
	originalVersion, err := activeVersion(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}

	cc := ChunkCoord{CX: 0, CY: 0}
	originalData, err := rdb.Get(ctx, versionChunkKey(originalVersion, cc)).Bytes()
	if err != nil {
		t.Fatal(err)
	}

	err = rollback.Rollback(ctx, targetID)
	if !errors.Is(err, ErrUnsupportedEvent) {
		t.Fatalf("Rollback error = %v, want ErrUnsupportedEvent", err)
	}

	// The active version must not have changed.
	activeVer, err := activeVersion(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}
	if activeVer != originalVersion {
		t.Errorf(
			"active version after failed rollback = %s, want %s",
			activeVer,
			originalVersion,
		)
	}

	// The original canvas data must remain unchanged.
	currentData, err := rdb.Get(ctx, versionChunkKey(originalVersion, cc)).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if string(currentData) != string(originalData) {
		t.Error("original canvas data changed after failed rollback")
	}

	// The original version must still list the origin chunk as open.
	open, err := rdb.SIsMember(ctx, versionChunksKey(originalVersion), cc.member()).Result()
	if err != nil || !open {
		t.Errorf("origin chunk in original version = %v, %v; want true, nil", open, err)
	}

	// No staging version should remain.
	counter, err := rdb.Get(ctx, versionCounterKey).Int64()
	if err != nil {
		t.Fatal(err)
	}

	stagingVersion := strconv.FormatInt(counter, 10)
	if stagingVersion != originalVersion {
		if exists, err := rdb.Exists(
			ctx,
			versionChunksKey(stagingVersion),
		).Result(); err != nil || exists != 0 {
			t.Errorf(
				"staging version chunk set exists = %d, %v; want 0, nil",
				exists,
				err,
			)
		}
	}
}

func TestReverseUndoRollbackAtLatestEventLeavesCanvasUnchanged(t *testing.T) {
	rdb, canvas, log, rollback := newTestRollback(t)
	ctx := context.Background()

	setTestPixel(t, rdb, 10, 10, "\xaa\xbb\xcc")
	targetID := appendTestPixelEvent(t, rdb, 10, 10, "#aabbcc", "#ffffff")

	if err := rollback.Rollback(ctx, targetID); err != nil {
		t.Fatalf("Rollback: %v", err)
	}

	if got := pixelColor(t, canvas, 10, 10); got != "#aabbcc" {
		t.Errorf("pixel after rollback = %s, want unchanged #aabbcc", got)
	}

	assertRollbackMarker(t, log, targetID)
}

func TestReverseUndoRollbackRejectsIncompleteConfiguration(t *testing.T) {
	var nilRollback *ReverseUndoRollback
	if err := nilRollback.Rollback(context.Background(), "1-0"); err == nil {
		t.Fatal("Rollback on nil receiver succeeded; want configuration error")
	}

	_, _, log, _ := newTestRollback(t)
	if err := (&ReverseUndoRollback{log: log}).Rollback(context.Background(), "1-0"); err == nil {
		t.Fatal("Rollback without canvas succeeded; want configuration error")
	}
	_, canvas, _, _ := newTestRollback(t)
	if err := (&ReverseUndoRollback{canvas: canvas}).Rollback(context.Background(), "1-0"); err == nil {
		t.Fatal("Rollback without event log succeeded; want configuration error")
	}
}

func TestReverseUndoRollbackErrorIncludesEventContext(t *testing.T) {
	rdb, _, _, rollback := newTestRollback(t)
	targetID := appendTestEvent(t, rdb, "checkpoint")
	badID := appendTestPixelEvent(t, rdb, 1, 1, "#000000", "not-a-color")

	err := rollback.Rollback(context.Background(), targetID)
	if !errors.Is(err, ErrInvalidColor) {
		t.Errorf("Rollback error = %v, want ErrInvalidColor (event %s)", err, badID)
	}
	if err == nil || !strings.Contains(err.Error(), badID) {
		t.Errorf("Rollback error %v does not identify event %s", err, badID)
	}
}

func TestCloneActiveCanvasVersion(t *testing.T) {
	rdb, canvas := newTestCanvas(t)
	ctx := context.Background()

	// Record the original active version.
	sourceVersion, err := activeVersion(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}

	// Put a non-white pixel in the origin chunk.
	setTestPixel(t, rdb, 1, 2, "#ff0000")

	// Add another chunk and give it clock metadata.
	addedCoord := ChunkCoord{CX: 1, CY: 0}
	wasAdded, err := canvas.AddChunk(ctx, addedCoord)
	if err != nil {
		t.Fatal(err)
	}
	if !wasAdded {
		t.Fatal("expected AddChunk to add a new chunk")
	}

	sourceClockKey := versionClockKey(sourceVersion, addedCoord)
	if err := rdb.HSet(ctx, sourceClockKey, "test-field", "test-value").Err(); err != nil {
		t.Fatal(err)
	}

	// Capture the original chunk data.
	originCoord := ChunkCoord{CX: 0, CY: 0}
	originBefore, err := rdb.Get(ctx, versionChunkKey(sourceVersion, originCoord)).Bytes()
	if err != nil {
		t.Fatal(err)
	}

	addedBefore, err := rdb.Get(ctx, versionChunkKey(sourceVersion, addedCoord)).Bytes()
	if err != nil {
		t.Fatal(err)
	}

	// Clone the active version.
	newVersion, err := cloneActiveCanvasVersion(ctx, rdb)
	if err != nil {
		t.Fatalf("clone active canvas version: %v", err)
	}

	if newVersion == sourceVersion {
		t.Fatalf("clone returned source version %q", sourceVersion)
	}

	// Cloning must not change the active version.
	activeAfter, err := activeVersion(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}
	if activeAfter != sourceVersion {
		t.Fatalf("active version changed from %q to %q during cloning",
			sourceVersion, activeAfter)
	}

	// Both chunks must exist in the cloned open-chunk set.
	newChunksKey := versionChunksKey(newVersion)
	for _, member := range []string{"0:0", "1:0"} {
		open, err := rdb.SIsMember(ctx, newChunksKey, member).Result()
		if err != nil {
			t.Fatal(err)
		}
		if !open {
			t.Errorf("chunk %q missing from cloned open-chunk set", member)
		}
	}

	// Chunk data must be identical.
	originAfter, err := rdb.Get(ctx, versionChunkKey(newVersion, originCoord)).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if string(originAfter) != string(originBefore) {
		t.Error("origin chunk data differs after cloning")
	}

	addedAfter, err := rdb.Get(ctx, versionChunkKey(newVersion, addedCoord)).Bytes()
	if err != nil {
		t.Fatal(err)
	}
	if string(addedAfter) != string(addedBefore) {
		t.Error("added chunk data differs after cloning")
	}

	// Clock metadata must also be preserved.
	clockValue, err := rdb.HGet(
		ctx,
		versionClockKey(newVersion, addedCoord),
		"test-field",
	).Result()
	if err != nil {
		t.Fatal(err)
	}
	if clockValue != "test-value" {
		t.Errorf("cloned clock value = %q, want %q", clockValue, "test-value")
	}
}

func TestClonedCanvasVersionIsIndependent(t *testing.T) {
	rdb, _ := newTestCanvas(t)
	ctx := context.Background()

	sourceVersion, err := activeVersion(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}

	cc := ChunkCoord{CX: 0, CY: 0}
	sourceChunkKey := versionChunkKey(sourceVersion, cc)

	// Capture the original chunk contents.
	sourceBefore, err := rdb.Get(ctx, sourceChunkKey).Bytes()
	if err != nil {
		t.Fatal(err)
	}

	// Clone the active canvas.
	newVersion, err := cloneActiveCanvasVersion(ctx, rdb)
	if err != nil {
		t.Fatalf("clone active canvas version: %v", err)
	}

	// Change one pixel in the cloned version only.
	offset := 0
	red := []byte{255, 0, 0}
	_, err = rdb.SetRange(
		ctx,
		versionChunkKey(newVersion, cc),
		int64(offset),
		string(red),
	).Result()
	if err != nil {
		t.Fatalf("modify cloned chunk: %v", err)
	}

	// The active version must remain unchanged.
	sourceAfter, err := rdb.Get(ctx, sourceChunkKey).Bytes()
	if err != nil {
		t.Fatal(err)
	}

	if string(sourceAfter) != string(sourceBefore) {
		t.Error("modifying cloned version changed the active chunk")
	}

	// The cloned version must contain the modification.
	clonedAfter, err := rdb.Get(ctx, versionChunkKey(newVersion, cc)).Bytes()
	if err != nil {
		t.Fatal(err)
	}

	if string(clonedAfter[:3]) != string(red) {
		t.Errorf("cloned pixel = %v, want %v", clonedAfter[:3], red)
	}

	// The active pointer must still reference the original version.
	activeAfter, err := activeVersion(ctx, rdb)
	if err != nil {
		t.Fatal(err)
	}
	if activeAfter != sourceVersion {
		t.Errorf("active version = %q, want %q", activeAfter, sourceVersion)
	}
}
