package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

const rollbackLockKey = "canvas:rollback_lock"

var (
	ErrRollbackInProgress   = errors.New("canvas rollback is in progress")
	ErrRollbackLockNotOwned = errors.New("rollback lock is no longer owned by this operation")
)

// RollbackLock is a Redis-backed lease. Its token prevents one operation from
// renewing or releasing a lock acquired by another operation.
type RollbackLock struct {
	rdb   *redis.Client
	key   string
	token string
	ttl   time.Duration
}

// AcquireRollbackLock acquires the canvas-wide rollback lease.
func AcquireRollbackLock(ctx context.Context, rdb *redis.Client, ttl time.Duration) (*RollbackLock, error) {
	if rdb == nil {
		return nil, errors.New("cannot acquire rollback lock: nil Redis client")
	}
	if ttl <= 0 {
		return nil, errors.New("rollback lock TTL must be positive")
	}

	tokenBytes := make([]byte, 16)
	if _, err := rand.Read(tokenBytes); err != nil {
		return nil, fmt.Errorf("generate rollback lock token: %w", err)
	}
	token := hex.EncodeToString(tokenBytes)

	acquired, err := rdb.SetNX(ctx, rollbackLockKey, token, ttl).Result()
	if err != nil {
		return nil, fmt.Errorf("acquire rollback lock: %w", err)
	}
	if !acquired {
		return nil, ErrRollbackInProgress
	}

	return &RollbackLock{rdb: rdb, key: rollbackLockKey, token: token, ttl: ttl}, nil
}

var renewRollbackLockScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('PEXPIRE', KEYS[1], ARGV[2])
end
return 0
`)

// Renew extends the lease only if this operation still owns it.
func (l *RollbackLock) Renew(ctx context.Context) error {
	if l == nil || l.rdb == nil {
		return errors.New("rollback lock is not configured")
	}
	ok, err := renewRollbackLockScript.Run(ctx, l.rdb, []string{l.key}, l.token, l.ttl.Milliseconds()).Int64()
	if err != nil {
		return fmt.Errorf("renew rollback lock: %w", err)
	}
	if ok != 1 {
		return ErrRollbackLockNotOwned
	}
	return nil
}

var releaseRollbackLockScript = redis.NewScript(`
if redis.call('GET', KEYS[1]) == ARGV[1] then
  return redis.call('DEL', KEYS[1])
end
return 0
`)

// Release deletes the lock only if this operation still owns it.
func (l *RollbackLock) Release(ctx context.Context) error {
	if l == nil || l.rdb == nil {
		return errors.New("rollback lock is not configured")
	}
	deleted, err := releaseRollbackLockScript.Run(ctx, l.rdb, []string{l.key}, l.token).Int64()
	if err != nil {
		return fmt.Errorf("release rollback lock: %w", err)
	}
	if deleted != 1 {
		return ErrRollbackLockNotOwned
	}
	return nil
}

// KeepAlive renews the lease periodically. The returned operation context is
// canceled if renewal fails. Call stop and check its error before treating the
// protected operation as successful.
func (l *RollbackLock) KeepAlive(ctx context.Context, interval time.Duration) (context.Context, func() error) {
	if interval <= 0 {
		interval = l.ttl / 3
		if interval <= 0 {
			interval = time.Millisecond
		}
	}
	operationCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-operationCtx.Done():
				done <- nil
				return
			case <-ticker.C:
				renewCtx, renewCancel := context.WithTimeout(context.Background(), interval)
				err := l.Renew(renewCtx)
				renewCancel()
				if err != nil {
					done <- err
					cancel()
					return
				}
			}
		}
	}()

	stop := func() error {
		cancel()
		return <-done
	}
	return operationCtx, stop
}
