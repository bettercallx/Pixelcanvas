package main

import (
	"context"
	"errors"
	"testing"
)

// fakeCanvas stands in for *Canvas: a chunk is open if it's in the map
type fakeCanvas map[ChunkCoord]bool

func (f fakeCanvas) IsOpen(_ context.Context, cc ChunkCoord) (bool, error) {
	return f[cc], nil
}

// brokenCanvas simulates Redis being down
type brokenCanvas struct{}

func (brokenCanvas) IsOpen(context.Context, ChunkCoord) (bool, error) {
	return false, errors.New("redis down")
}

func TestAdjacentPolicy(t *testing.T) {
	p := &AdjacentPolicy{canvas: fakeCanvas{{0, 0}: true}}
	tests := []struct {
		cc   ChunkCoord
		want bool
	}{
		{ChunkCoord{1, 0}, true},
		{ChunkCoord{-1, 0}, true},
		{ChunkCoord{0, -1}, true},
		{ChunkCoord{0, 1}, true},
		{ChunkCoord{1, 1}, false}, // diagonal only touches a corner
		{ChunkCoord{5, 5}, false},
	}
	for _, tt := range tests {
		got, err := p.CanAdd(context.Background(), tt.cc)
		if err != nil || got != tt.want {
			t.Errorf("CanAdd(%v) = %v, %v; want %v, nil", tt.cc, got, err, tt.want)
		}
	}
}

func TestAdjacentPolicyRedisError(t *testing.T) {
	p := &AdjacentPolicy{canvas: brokenCanvas{}}
	if ok, err := p.CanAdd(context.Background(), ChunkCoord{1, 0}); err == nil || ok {
		t.Errorf("CanAdd = %v, %v; want false and an error", ok, err)
	}
}

func TestFreePolicy(t *testing.T) {
	p := &FreePolicy{}
	if ok, err := p.CanAdd(context.Background(), ChunkCoord{100, -100}); err != nil || !ok {
		t.Errorf("CanAdd = %v, %v; want true, nil", ok, err)
	}
}

func TestNewChunkPolicy(t *testing.T) {
	if p, err := NewChunkPolicy("adjacent", fakeCanvas{}); err != nil {
		t.Errorf("adjacent: %v", err)
	} else if _, ok := p.(*AdjacentPolicy); !ok {
		t.Errorf("adjacent: got %T", p)
	}

	if p, err := NewChunkPolicy("free", fakeCanvas{}); err != nil {
		t.Errorf("free: %v", err)
	} else if _, ok := p.(*FreePolicy); !ok {
		t.Errorf("free: got %T", p)
	}

	if _, err := NewChunkPolicy("adjcent", fakeCanvas{}); err == nil {
		t.Error("typo in CHUNK_POLICY should be an error")
	}
}
