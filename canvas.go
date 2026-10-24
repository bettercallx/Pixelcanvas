// single pixel
package main

import "sync"

type PixelEvent struct {
	X         int    `json:"x"`
	Y         int    `json:"y"`
	Color     string `json:"color"`
	UserID    string `json:"user_id"`
	Timestamp int64  `json:"timestamp"`
}

// canvas state
type Canvas struct {
	Width  int
	Height int
	Redis  *redis.Client
	mu     sync.RWMutex
}

// TODO: implement
// func NewCanvas(width, height int) *Canvas
// func (c *Canvas) GetPixel(x, y int) string
// func (c *Canvas) SetPixel(event PixelEvent) error
// func (c *Canvas) Snapshot() [][]string // return a copy of current canvas state
