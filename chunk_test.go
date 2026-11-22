package main

import "testing"

func TestFloorDiv(t *testing.T) {
	tests := []struct {
		a, b, want int
	}{
		{0, 64, 0},
		{63, 64, 0},
		{64, 64, 1},
		{-1, 64, -1}, // Go's -1/64 would give 0
		{-64, 64, -1},
		{-65, 64, -2},
	}
	for _, tt := range tests {
		if got := floorDiv(tt.a, tt.b); got != tt.want {
			t.Errorf("floorDiv(%d, %d) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestLocate(t *testing.T) {
	last := int64(chunkBytes - bytesPerPixel) // offset of pixel (63,63) inside a chunk
	tests := []struct {
		x, y       int
		wantChunk  ChunkCoord
		wantOffset int64
	}{
		{0, 0, ChunkCoord{0, 0}, 0},
		{1, 0, ChunkCoord{0, 0}, 3},             // next pixel in the row
		{0, 1, ChunkCoord{0, 0}, ChunkSize * 3}, // next row
		{63, 63, ChunkCoord{0, 0}, last},
		{64, 0, ChunkCoord{1, 0}, 0},
		{-1, 0, ChunkCoord{-1, 0}, 63 * 3}, // right edge of the chunk to the left
		{-1, -1, ChunkCoord{-1, -1}, last},
		{-64, 0, ChunkCoord{-1, 0}, 0},
		{-65, 0, ChunkCoord{-2, 0}, 63 * 3},
	}
	for _, tt := range tests {
		gotChunk, gotOffset := locate(tt.x, tt.y)
		if gotChunk != tt.wantChunk || gotOffset != tt.wantOffset {
			t.Errorf("locate(%d, %d) = %v, %d; want %v, %d",
				tt.x, tt.y, gotChunk, gotOffset, tt.wantChunk, tt.wantOffset)
		}
	}
}

// every pixel must land inside its chunk on a pixel boundary, negatives included
func TestLocateStaysInChunk(t *testing.T) {
	for x := -200; x <= 200; x++ {
		for y := -200; y <= 200; y++ {
			_, off := locate(x, y)
			if off < 0 || off >= chunkBytes || off%bytesPerPixel != 0 {
				t.Fatalf("locate(%d, %d) offset %d out of range", x, y, off)
			}
		}
	}
}

func TestParseHexColor(t *testing.T) {
	valid := []struct {
		in   string
		want [3]byte
	}{
		{"#ff8800", [3]byte{0xff, 0x88, 0x00}},
		{"#FF8800", [3]byte{0xff, 0x88, 0x00}},
		{"#000000", [3]byte{0, 0, 0}},
	}
	for _, tt := range valid {
		got, err := parseHexColor(tt.in)
		if err != nil || got != tt.want {
			t.Errorf("parseHexColor(%q) = %v, %v; want %v, nil", tt.in, got, err, tt.want)
		}
	}

	invalid := []string{"", "ff8800", "#fff", "#ff88001", "#gg0000", "red"}
	for _, in := range invalid {
		if _, err := parseHexColor(in); err != ErrInvalidColor {
			t.Errorf("parseHexColor(%q) err = %v, want ErrInvalidColor", in, err)
		}
	}
}

func TestChunkCoordKey(t *testing.T) {
	cc := ChunkCoord{-1, 2}
	if got := cc.key(); got != "canvas:chunk:-1:2" {
		t.Errorf("key() = %q", got)
	}
}
