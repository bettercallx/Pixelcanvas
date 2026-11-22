// chunk placement rules
package main

import (
	"context"
	"fmt"
)

// ChunkPolicy decides whether a new chunk may be opened at a position
type ChunkPolicy interface {
	CanAdd(ctx context.Context, cc ChunkCoord) (bool, error)
}

// policy don't need to store state, just check the canvas for open chunks
type openChecker interface {
	IsOpen(ctx context.Context, cc ChunkCoord) (bool, error)
}

// maps ChunkPolicy to adjacent/free modes
func NewChunkPolicy(name string, canvas openChecker) (ChunkPolicy, error) {
	switch name {
	case "adjacent":
		return &AdjacentPolicy{canvas: canvas}, nil
	case "free":
		return &FreePolicy{}, nil
	default:
		return nil, fmt.Errorf("unknown CHUNK_POLICY %q (want adjacent|free)", name)
	}
}

// AdjacentPolicy: a new chunk must share an edge with an open chunk
type AdjacentPolicy struct {
	canvas openChecker
}

func (p *AdjacentPolicy) CanAdd(ctx context.Context, cc ChunkCoord) (bool, error) {
	for _, n := range cc.Neighbors() {
		open, err := p.canvas.IsOpen(ctx, n)
		if err != nil {
			return false, err
		}
		if open {
			return true, nil
		}
	}
	return false, nil
}

// FreePolicy: a new chunk may go anywhere
type FreePolicy struct{}

func (p *FreePolicy) CanAdd(ctx context.Context, cc ChunkCoord) (bool, error) {
	return true, nil
}
