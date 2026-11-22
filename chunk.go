// chunk geometry and pixel types
package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const (
	ChunkSize     = 64 // 64x64 pixels per chunk
	bytesPerPixel = 3  // R, G, B
	chunkBytes    = ChunkSize * ChunkSize * bytesPerPixel
)

var ErrInvalidColor = errors.New("invalid color, want #rrggbb")

// single pixel
type PixelEvent struct {
	X         int    `json:"x"`
	Y         int    `json:"y"`
	Color     string `json:"color"` // "#rrggbb"
	UserID    string `json:"user_id"`
	Timestamp int64  `json:"timestamp"` // set by server
}

type ChunkCoord struct {
	CX int `json:"cx"`
	CY int `json:"cy"`
}

func (c ChunkCoord) member() string { return fmt.Sprintf("%d:%d", c.CX, c.CY) }
func (c ChunkCoord) key() string    { return "canvas:chunk:" + c.member() }

// the 4 chunks sharing an edge with c
func (c ChunkCoord) Neighbors() [4]ChunkCoord {
	return [4]ChunkCoord{
		{c.CX - 1, c.CY}, {c.CX + 1, c.CY},
		{c.CX, c.CY - 1}, {c.CX, c.CY + 1},
	}
}

// locate maps a global pixel coordinate to its chunk and byte offset inside that chunk
func locate(x, y int) (ChunkCoord, int64) {
	cc := ChunkCoord{floorDiv(x, ChunkSize), floorDiv(y, ChunkSize)}
	lx, ly := x-cc.CX*ChunkSize, y-cc.CY*ChunkSize
	return cc, int64((ly*ChunkSize + lx) * bytesPerPixel)
}

// floorDiv rounds toward negative infinity; Go's / rounds toward zero (-1/64 == 0)
func floorDiv(a, b int) int {
	q := a / b
	if a%b != 0 && a < 0 {
		q--
	}
	return q
}

// "#ff8800" -> [0xff, 0x88, 0x00]
func parseHexColor(s string) ([3]byte, error) {
	var rgb [3]byte
	if len(s) != 7 || !strings.HasPrefix(s, "#") {
		return rgb, ErrInvalidColor
	}
	if _, err := hex.Decode(rgb[:], []byte(s[1:])); err != nil {
		return rgb, ErrInvalidColor
	}
	return rgb, nil
}
