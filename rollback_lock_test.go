package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func TestAcquireRollbackLockExcludesSecondOwner(t *testing.T) {
	rdb, _ := newTestCanvas(t)
	ctx := context.Background()

	lock, err := AcquireRollbackLock(ctx, rdb, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireRollbackLock(ctx, rdb, time.Second); !errors.Is(err, ErrRollbackInProgress) {
		t.Fatalf("second acquisition error = %v, want ErrRollbackInProgress", err)
	}
	if err := lock.Release(ctx); err != nil {
		t.Fatal(err)
	}

	second, err := AcquireRollbackLock(ctx, rdb, time.Second)
	if err != nil {
		t.Fatalf("acquire after release: %v", err)
	}
	if err := second.Release(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestRollbackLockRenewAndReleaseRequireOwnership(t *testing.T) {
	rdb, _ := newTestCanvas(t)
	ctx := context.Background()
	lock, err := AcquireRollbackLock(ctx, rdb, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Simulate the lease expiring and another operation acquiring the key.
	if err := rdb.Set(ctx, rollbackLockKey, "another-owner", time.Second).Err(); err != nil {
		t.Fatal(err)
	}
	if err := lock.Renew(ctx); !errors.Is(err, ErrRollbackLockNotOwned) {
		t.Errorf("Renew error = %v, want ErrRollbackLockNotOwned", err)
	}
	if err := lock.Release(ctx); !errors.Is(err, ErrRollbackLockNotOwned) {
		t.Errorf("Release error = %v, want ErrRollbackLockNotOwned", err)
	}
	if got, err := rdb.Get(ctx, rollbackLockKey).Result(); err != nil || got != "another-owner" {
		t.Errorf("other owner's lock = %q, %v; want it preserved", got, err)
	}
}

func TestRollbackLockRenewExtendsLease(t *testing.T) {
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })

	ctx := context.Background()

	lock, err := AcquireRollbackLock(ctx, rdb, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Advance close to the original expiration.
	mr.FastForward(800 * time.Millisecond)

	// Renew the lease for another second.
	if err := lock.Renew(ctx); err != nil {
		t.Fatal(err)
	}

	// The original lease would have expired, but the renewed one should remain.
	mr.FastForward(800 * time.Millisecond)

	if !mr.Exists(rollbackLockKey) {
		t.Fatal("lock expired before renewed TTL elapsed")
	}

	// Advance beyond the renewed expiration.
	mr.FastForward(300 * time.Millisecond)

	if mr.Exists(rollbackLockKey) {
		t.Fatal("lock still exists after renewed TTL elapsed")
	}
}

func TestPixelWritesAreRejectedDuringRollback(t *testing.T) {
	for _, mode := range []string{"sequencer", "lww"} {
		t.Run(mode, func(t *testing.T) {
			rdb, _ := newTestCanvas(t)
			ctx := context.Background()
			resolver, err := NewConflictResolver(mode, rdb, "server-1")
			if err != nil {
				t.Fatal(err)
			}
			lock, err := AcquireRollbackLock(ctx, rdb, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := resolver.Apply(ctx, px(1, 1, "#112233")); !errors.Is(err, ErrRollbackInProgress) {
				t.Fatalf("Apply error = %v, want ErrRollbackInProgress", err)
			}
			if err := lock.Release(ctx); err != nil {
				t.Fatal(err)
			}
			if _, err := resolver.Apply(ctx, px(1, 1, "#112233")); err != nil {
				t.Fatalf("Apply after lock release: %v", err)
			}
		})
	}
}

func TestChunkAddIsRejectedDuringRollback(t *testing.T) {
	_, canvas := newTestCanvas(t)
	ctx := context.Background()
	lock, err := AcquireRollbackLock(ctx, canvas.rdb, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	if added, err := canvas.AddChunk(ctx, ChunkCoord{CX: 1, CY: 0}); added || !errors.Is(err, ErrRollbackInProgress) {
		t.Fatalf("AddChunk = (%v, %v), want (false, ErrRollbackInProgress)", added, err)
	}
	if err := lock.Release(ctx); err != nil {
		t.Fatal(err)
	}
	if added, err := canvas.AddChunk(ctx, ChunkCoord{CX: 1, CY: 0}); !added || err != nil {
		t.Fatalf("AddChunk after lock release = (%v, %v), want (true, nil)", added, err)
	}
}

func TestReverseRollbackReleasesLockAfterInvalidTarget(t *testing.T) {
	rdb, canvas := newTestCanvas(t)
	rollback := NewReverseUndoRollback(canvas, NewEventLog(rdb))
	if err := rollback.Rollback(context.Background(), "9999999999999-0"); !errors.Is(err, ErrInvalidRollbackTarget) {
		t.Fatalf("Rollback error = %v, want ErrInvalidRollbackTarget", err)
	}

	lock, err := AcquireRollbackLock(context.Background(), rdb, time.Second)
	if err != nil {
		t.Fatalf("rollback did not release its lock after failure: %v", err)
	}
	if err := lock.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
}
