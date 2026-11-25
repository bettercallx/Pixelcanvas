// append-only log of every canvas change in a Redis Stream; William's rollback feature reads it
// pixel entries are written by the scripts in conflict.go
// chunk_added entries by appendChunkEvent. Field names must match parseEvent below.
package main

import (
	"context"
	"strconv"

	"github.com/redis/go-redis/v9"
)

const eventStreamKey = "canvas:events"

// LoggedEvent is one entry read back from the log. Pixel events use X/Y/Color/PrevColor/UserID/Timestamp,
// chunk_added events use CX/CY.
type LoggedEvent struct {
	ID        string `json:"id"`   // stream ID "<ms>-<seq>" from Redis' clock: one global order across servers
	Type      string `json:"type"` // "pixel" / "chunk_added" / "rollback"
	X         int    `json:"x"`    // 0 is valid
	Y         int    `json:"y"`
	Color     string `json:"color"`
	PrevColor string `json:"prev_color"` // color before this write; undo = set the pixel back to this
	UserID    string `json:"user_id"`
	Timestamp int64  `json:"timestamp"`
	CX        int    `json:"cx"`
	CY        int    `json:"cy"`
	Seq       int64  `json:"seq,omitempty"`
	Lamport   uint64 `json:"lamport,omitempty"`
	ServerID  string `json:"server_id,omitempty"`
	TargetID  string `json:"target_id,omitempty"` //ID targeted by rollback
}

// XAdd is append only, event is chunked_added
// the same MULTI/EXEC to ensure the chunk is open and the log is written atomically.
func appendChunkEvent(ctx context.Context, p redis.Pipeliner, cc ChunkCoord) {
	p.XAdd(ctx, &redis.XAddArgs{
		Stream: eventStreamKey,
		Values: []any{"type", "chunk_added", "cx", cc.CX, "cy", cc.CY},
	})
}

func appendRollbackEvent(ctx context.Context, p redis.Pipeliner, targetID string) {
	p.XAdd(ctx, &redis.XAddArgs{
		Stream: eventStreamKey,
		Values: []any{"type", "rollback", "target_id", targetID},
	})
}

// EventLog is the read side for rollback
type EventLog struct {
	rdb *redis.Client
}

func NewEventLog(rdb *redis.Client) *EventLog {
	return &EventLog{rdb: rdb}
}

// Range returns events with IDs in [start, end], oldest first.
// "-" / "+" mean the start / end of the log; a bare ms timestamp like "1795315689475" also works.
// count <= 0 means no limit. To page: call again with start = "(" + last ID (exclusive).
func (l *EventLog) Range(ctx context.Context, start, end string, count int64) ([]LoggedEvent, error) {
	var msgs []redis.XMessage
	var err error
	if count > 0 {
		msgs, err = l.rdb.XRangeN(ctx, eventStreamKey, start, end, count).Result()
	} else {
		msgs, err = l.rdb.XRange(ctx, eventStreamKey, start, end).Result()
	}
	if err != nil {
		return nil, err
	}
	return parseEvents(msgs), nil
}

// Range newest first
func (l *EventLog) RevRange(ctx context.Context, end, start string, count int64) ([]LoggedEvent, error) {
	var msgs []redis.XMessage
	var err error
	if count > 0 {
		msgs, err = l.rdb.XRevRangeN(ctx, eventStreamKey, end, start, count).Result()
	} else {
		msgs, err = l.rdb.XRevRange(ctx, eventStreamKey, end, start).Result()
	}
	if err != nil {
		return nil, err
	}
	return parseEvents(msgs), nil
}

func parseEvents(msgs []redis.XMessage) []LoggedEvent {
	events := make([]LoggedEvent, 0, len(msgs))
	for _, m := range msgs {
		events = append(events, parseEvent(m))
	}
	return events
}

// Redis returns every stream field as a string
func parseEvent(m redis.XMessage) LoggedEvent {
	str := func(k string) string { s, _ := m.Values[k].(string); return s }
	num := func(k string) int { n, _ := strconv.Atoi(str(k)); return n }

	ts, _ := strconv.ParseInt(str("timestamp"), 10, 64)
	seq, _ := strconv.ParseInt(str("seq"), 10, 64)
	lamport, _ := strconv.ParseUint(str("lamport"), 10, 64)
	return LoggedEvent{
		ID:        m.ID,
		Type:      str("type"),
		X:         num("x"),
		Y:         num("y"),
		Color:     str("color"),
		PrevColor: str("prev_color"),
		UserID:    str("user_id"),
		Timestamp: ts,
		CX:        num("cx"),
		CY:        num("cy"),
		Seq:       seq,
		Lamport:   lamport,
		ServerID:  str("server_id"),
		TargetID:  str("target_id"),
	}
}
