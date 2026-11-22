// ordering of concurrent writes to the same pixel: sequencer or lww (CONFLICT_MODE)
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

const seqKey = "canvas:seq"

var ErrStaleWrite = errors.New("write lost to a newer write on the same pixel")

type ConflictResolver interface {
	Name() string
	// Apply writes ev and returns it with its ordering stamp set
	Apply(ctx context.Context, ev PixelEvent) (PixelEvent, error)
	// Observe sees every message received over Pub/Sub
	Observe(raw []byte)
}

func NewConflictResolver(mode string, rdb *redis.Client, serverID string) (ConflictResolver, error) {
	switch mode {
	case "sequencer":
		return &SequencerResolver{rdb: rdb}, nil
	case "lww":
		if serverID == "" {
			return nil, errors.New("lww mode needs a unique SERVER_ID")
		}
		return &LWWResolver{rdb: rdb, serverID: serverID}, nil
	default:
		return nil, fmt.Errorf("unknown CONFLICT_MODE %q (want sequencer|lww)", mode)
	}
}

// Supersedes reports whether a should replace b on the same pixel
func Supersedes(a, b PixelEvent) bool {
	if a.Seq != 0 || b.Seq != 0 {
		return a.Seq > b.Seq
	}
	if a.Lamport != b.Lamport {
		return a.Lamport > b.Lamport
	}
	return a.ServerID > b.ServerID
}

// SequencerResolver: Redis gives every write a global sequence number
type SequencerResolver struct {
	rdb *redis.Client
}

func (r *SequencerResolver) Name() string { return "sequencer" }

var sequencerScript = redis.NewScript(`
-- KEYS: chunks set, chunk, events stream, seq counter
-- ARGV: chunk member, byte offset, 3 raw color bytes, x, y, user_id, timestamp

local function hex(rgb)
  return string.format('#%02x%02x%02x', string.byte(rgb, 1, 3))
end

if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  return false
end

local seq = redis.call('INCR', KEYS[4])
local offset = tonumber(ARGV[2])
local old = redis.call('GETRANGE', KEYS[2], offset, offset + 2)
redis.call('SETRANGE', KEYS[2], offset, ARGV[3])
redis.call('XADD', KEYS[3], '*',
  'type', 'pixel',
  'x', ARGV[4], 'y', ARGV[5],
  'color', hex(ARGV[3]),
  'prev_color', hex(old),
  'user_id', ARGV[6],
  'timestamp', ARGV[7],
  'seq', seq)
return seq
`)

func (r *SequencerResolver) Apply(ctx context.Context, ev PixelEvent) (PixelEvent, error) {
	rgb, err := parseHexColor(ev.Color)
	if err != nil {
		return ev, err
	}
	cc, offset := locate(ev.X, ev.Y)

	keys := []string{chunksSetKey, cc.key(), eventStreamKey, seqKey}
	seq, err := sequencerScript.Run(ctx, r.rdb, keys,
		cc.member(), offset, string(rgb[:]),
		ev.X, ev.Y, ev.UserID, ev.Timestamp,
	).Int64()
	if errors.Is(err, redis.Nil) {
		return ev, ErrChunkNotOpen
	}
	if err != nil {
		return ev, err
	}
	ev.Seq = seq
	return ev, nil
}

func (r *SequencerResolver) Observe([]byte) {}

// LWWResolver: last-writer-wins on (lamport, server_id)
type LWWResolver struct {
	rdb      *redis.Client
	serverID string
	clock    LamportClock
}

func (r *LWWResolver) Name() string { return "lww" }

// hash of pixel offset -> "lamport:server_id" of the stored write
func clockKey(cc ChunkCoord) string { return "canvas:clock:" + cc.member() }

// writes only if the stamp beats the stored one; returns {1}, or {0, stored stamp}
var lwwScript = redis.NewScript(`
-- KEYS: chunks set, chunk, events stream, chunk's clock hash
-- ARGV: chunk member, byte offset, 3 raw color bytes, lamport, server_id, x, y, user_id, timestamp

local function hex(rgb)
  return string.format('#%02x%02x%02x', string.byte(rgb, 1, 3))
end

if redis.call('SISMEMBER', KEYS[1], ARGV[1]) == 0 then
  return false
end

local lamport = tonumber(ARGV[4])
local stored = redis.call('HGET', KEYS[4], ARGV[2])
if stored then
  local sep = string.find(stored, ':', 1, true)
  local sl = tonumber(string.sub(stored, 1, sep - 1))
  local ss = string.sub(stored, sep + 1)
  if lamport < sl or (lamport == sl and ARGV[5] <= ss) then
    return {0, stored}
  end
end

local offset = tonumber(ARGV[2])
local old = redis.call('GETRANGE', KEYS[2], offset, offset + 2)
redis.call('SETRANGE', KEYS[2], offset, ARGV[3])
redis.call('HSET', KEYS[4], ARGV[2], ARGV[4] .. ':' .. ARGV[5])
redis.call('XADD', KEYS[3], '*',
  'type', 'pixel',
  'x', ARGV[6], 'y', ARGV[7],
  'color', hex(ARGV[3]),
  'prev_color', hex(old),
  'user_id', ARGV[8],
  'timestamp', ARGV[9],
  'lamport', ARGV[4],
  'server_id', ARGV[5])
return {1}
`)

func (r *LWWResolver) Apply(ctx context.Context, ev PixelEvent) (PixelEvent, error) {
	rgb, err := parseHexColor(ev.Color)
	if err != nil {
		return ev, err
	}
	cc, offset := locate(ev.X, ev.Y)

	ev.Lamport = r.clock.Tick()
	ev.ServerID = r.serverID

	keys := []string{chunksSetKey, cc.key(), eventStreamKey, clockKey(cc)}
	res, err := lwwScript.Run(ctx, r.rdb, keys,
		cc.member(), offset, string(rgb[:]),
		ev.Lamport, ev.ServerID,
		ev.X, ev.Y, ev.UserID, ev.Timestamp,
	).Slice()
	if errors.Is(err, redis.Nil) {
		return ev, ErrChunkNotOpen
	}
	if err != nil {
		return ev, err
	}
	if won, _ := res[0].(int64); won == 1 {
		return ev, nil
	}
	// catch up to the winner so our next write can beat it
	if len(res) > 1 {
		if stored, ok := res[1].(string); ok {
			if l, _, ok := parseStamp(stored); ok {
				r.clock.Observe(l)
			}
		}
	}
	return ev, ErrStaleWrite
}

func (r *LWWResolver) Observe(raw []byte) {
	var m struct {
		Type    string `json:"type"`
		Lamport uint64 `json:"lamport"`
	}
	if json.Unmarshal(raw, &m) == nil && m.Type == "pixel" {
		r.clock.Observe(m.Lamport)
	}
}

// "12:s1" -> 12, "s1"
func parseStamp(s string) (uint64, string, bool) {
	l, id, ok := strings.Cut(s, ":")
	if !ok {
		return 0, "", false
	}
	n, err := strconv.ParseUint(l, 10, 64)
	return n, id, err == nil
}

type LamportClock struct {
	t atomic.Uint64
}

func (c *LamportClock) Tick() uint64 { return c.t.Add(1) }

// Observe makes the next Tick bigger than t
func (c *LamportClock) Observe(t uint64) {
	for {
		cur := c.t.Load()
		if t <= cur || c.t.CompareAndSwap(cur, t) {
			return
		}
	}
}

func (c *LamportClock) Now() uint64 { return c.t.Load() }
