// cross-server broadcast over Redis Pub/Sub
package main

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// Broadcaster delivers a message to the clients on every server
type Broadcaster interface {
	// Start subscribes and forwards every received message to the local hub
	Start(ctx context.Context) error
	// Publish sends msg to all servers; cc is the chunk the event belongs to (used by sharded mode)
	Publish(ctx context.Context, cc ChunkCoord, msg []byte) error
}

// maps BROADCAST_MODE to an implementation
func NewBroadcaster(mode string, rdb *redis.Client, hub *Hub) (Broadcaster, error) {
	switch mode {
	case "global":
		return &GlobalBroadcaster{rdb: rdb, hub: hub}, nil
	default:
		return nil, fmt.Errorf("unknown BROADCAST_MODE %q (want global)", mode)
	}
}

const globalChannel = "pixels"

// GlobalBroadcaster: one channel for everything, every server receives every event
type GlobalBroadcaster struct {
	rdb *redis.Client
	hub *Hub
}

func (b *GlobalBroadcaster) Start(ctx context.Context) error {
	sub := b.rdb.Subscribe(ctx, globalChannel)
	// Subscribe doesn't wait; wait for Redis to confirm so we never serve clients
	// before we can hear the other servers
	if _, err := sub.Receive(ctx); err != nil {
		sub.Close()
		return fmt.Errorf("subscribe %s: %w", globalChannel, err)
	}

	go func() {
		// go-redis reconnects on its own; messages published while disconnected are lost
		for m := range sub.Channel() {
			b.hub.Broadcast <- []byte(m.Payload)
		}
	}()
	return nil
}

func (b *GlobalBroadcaster) Publish(ctx context.Context, _ ChunkCoord, msg []byte) error {
	return b.rdb.Publish(ctx, globalChannel, msg).Err()
}
