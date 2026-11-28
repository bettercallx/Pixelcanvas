package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// TEST_REDIS_ADDR runs against a real Redis instead of miniredis; it gets flushed
func newTestCanvas(t *testing.T) (*redis.Client, *Canvas) {
	t.Helper()
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		addr = miniredis.RunT(t).Addr()
	}
	rdb := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { rdb.Close() })
	if err := rdb.FlushDB(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	canvas, err := NewCanvas(context.Background(), rdb, 10)
	if err != nil {
		t.Fatal(err)
	}
	return rdb, canvas
}

func pixelColor(t *testing.T, canvas *Canvas, x, y int) string {
	t.Helper()
	cc, off := locate(x, y)
	data, err := canvas.GetChunk(context.Background(), cc)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("#%02x%02x%02x", data[off], data[off+1], data[off+2])
}

func px(x, y int, color string) PixelEvent { return PixelEvent{X: x, Y: y, Color: color, UserID: "u"} }

func TestSupersedes(t *testing.T) {
	tests := []struct {
		name string
		a, b PixelEvent
		want bool
	}{
		{"higher seq wins", PixelEvent{Seq: 5}, PixelEvent{Seq: 4}, true},
		{"lower seq loses", PixelEvent{Seq: 4}, PixelEvent{Seq: 5}, false},
		{"seq beats empty", PixelEvent{Seq: 1}, PixelEvent{}, true},
		{"higher lamport wins", PixelEvent{Lamport: 3, ServerID: "a"}, PixelEvent{Lamport: 2, ServerID: "z"}, true},
		{"lamport tie: bigger server id wins", PixelEvent{Lamport: 3, ServerID: "b"}, PixelEvent{Lamport: 3, ServerID: "a"}, true},
		{"identical event does not supersede itself", PixelEvent{Lamport: 3, ServerID: "a"}, PixelEvent{Lamport: 3, ServerID: "a"}, false},
	}
	for _, tt := range tests {
		if got := Supersedes(tt.a, tt.b); got != tt.want {
			t.Errorf("%s: Supersedes = %v, want %v", tt.name, got, tt.want)
		}
	}
}

func TestLamportClock(t *testing.T) {
	var c LamportClock
	if c.Tick() != 1 || c.Tick() != 2 {
		t.Fatal("Tick should count up from 1")
	}
	c.Observe(10)
	if got := c.Tick(); got != 11 {
		t.Errorf("after Observe(10), Tick = %d, want 11", got)
	}
	c.Observe(5)
	if got := c.Tick(); got != 12 {
		t.Errorf("after Observe(5), Tick = %d, want 12", got)
	}

	var wg sync.WaitGroup
	seen := sync.Map{}
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if _, dup := seen.LoadOrStore(c.Tick(), true); dup {
					t.Error("duplicate Lamport time")
				}
			}
		}()
	}
	wg.Wait()
}

func TestSequencerApply(t *testing.T) {
	rdb, canvas := newTestCanvas(t)
	r, _ := NewConflictResolver("sequencer", rdb, "s1")
	ctx := context.Background()

	for i, color := range []string{"#ff0000", "#00ff00", "#0000ff"} {
		ev := px(1, 2, color)

		ev, err := r.Apply(ctx, ev)
		if err != nil {
			t.Fatal(err)
		}
		if ev.Seq != int64(i+1) {
			t.Errorf("write %d got seq %d, want %d", i, ev.Seq, i+1)
		}
	}

	if got := pixelColor(t, canvas, 1, 2); got != "#0000ff" {
		t.Errorf("pixel = %s, want #0000ff", got)
	}

	events, err := NewEventLog(rdb).Range(ctx, "-", "+", 0)
	if err != nil {
		t.Fatal(err)
	}
	last := events[len(events)-1]
	if last.Seq != 3 || last.Color != "#0000ff" || last.PrevColor != "#00ff00" {
		t.Errorf("last log entry = %+v", last)
	}

	_, seq, err := canvas.GetChunkWithSeq(ctx, ChunkCoord{0, 0})
	if err != nil || seq != 3 {
		t.Errorf("GetChunkWithSeq seq = %d, %v; want 3", seq, err)
	}

	_, closedErr := r.Apply(ctx, px(500, 500, "#000000"))
	if !errors.Is(closedErr, ErrChunkNotOpen) {
		t.Errorf("closed chunk error = %v, want %v", closedErr, ErrChunkNotOpen)
	}

	_, colorErr := r.Apply(ctx, px(0, 0, "red"))
	if !errors.Is(colorErr, ErrInvalidColor) {
		t.Errorf("invalid color error = %v, want %v", colorErr, ErrInvalidColor)
	}
}

func TestLWWApplyRejectsOlderStamp(t *testing.T) {
	rdb, canvas := newTestCanvas(t)
	ctx := context.Background()
	ahead := &LWWResolver{rdb: rdb, serverID: "s1"}
	behind := &LWWResolver{rdb: rdb, serverID: "s2"}
	ahead.clock.Observe(100)

	if _, err := ahead.Apply(ctx, px(3, 3, "#ff0000")); err != nil {
		t.Fatal(err)
	}
	if _, err := behind.Apply(ctx, px(3, 3, "#00ff00")); !errors.Is(err, ErrStaleWrite) {
		t.Fatalf("err = %v, want ErrStaleWrite", err)
	}
	if got := pixelColor(t, canvas, 3, 3); got != "#ff0000" {
		t.Errorf("pixel = %s, want #ff0000", got)
	}
	// losing advanced s2's clock, so the retry wins
	ev, err := behind.Apply(ctx, px(3, 3, "#00ff00"))
	if err != nil {
		t.Fatalf("retry after catching up: %v", err)
	}
	if ev.Lamport <= 101 {
		t.Errorf("retry stamp %d should be past the stored 101", ev.Lamport)
	}
	if got := pixelColor(t, canvas, 3, 3); got != "#00ff00" {
		t.Errorf("pixel = %s, want #00ff00", got)
	}

	events, _ := NewEventLog(rdb).Range(ctx, "-", "+", 0)
	var pixels []LoggedEvent
	for _, e := range events {
		if e.Type == "pixel" {
			pixels = append(pixels, e)
		}
	}
	if len(pixels) != 2 || pixels[0].ServerID != "s1" || pixels[1].ServerID != "s2" {
		t.Errorf("logged pixel events = %+v", pixels)
	}
}

func TestLWWTieBrokenByServerID(t *testing.T) {
	rdb, canvas := newTestCanvas(t)
	ctx := context.Background()
	a := &LWWResolver{rdb: rdb, serverID: "a"}
	b := &LWWResolver{rdb: rdb, serverID: "b"}

	// both at Lamport time 1, so the bigger server id wins
	if _, err := b.Apply(ctx, px(0, 0, "#bbbbbb")); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Apply(ctx, px(0, 0, "#aaaaaa")); !errors.Is(err, ErrStaleWrite) {
		t.Fatalf("err = %v, want ErrStaleWrite", err)
	}
	if got := pixelColor(t, canvas, 0, 0); got != "#bbbbbb" {
		t.Errorf("pixel = %s, want #bbbbbb", got)
	}
}

func TestLWWObserve(t *testing.T) {
	r := &LWWResolver{serverID: "s1"}
	msg, _ := json.Marshal(pixelMessage{Type: "pixel", PixelEvent: PixelEvent{Lamport: 42, ServerID: "s2"}})
	r.Observe(msg)
	r.Observe([]byte(`{"type":"chunk_added","cx":1,"cy":0}`))
	r.Observe([]byte(`not json`))
	if got := r.clock.Tick(); got != 43 {
		t.Errorf("Tick after observing 42 = %d, want 43", got)
	}
}

type client struct {
	pixels map[[2]int]PixelEvent
}

func (c *client) receive(ev PixelEvent) {
	k := [2]int{ev.X, ev.Y}
	if cur, ok := c.pixels[k]; !ok || Supersedes(ev, cur) {
		c.pixels[k] = ev
	}
}

// every client gets the events in a different order and must still match Redis
func TestConvergenceUnderContention(t *testing.T) {
	for _, mode := range []string{"sequencer", "lww"} {
		t.Run(mode, func(t *testing.T) {
			rdb, canvas := newTestCanvas(t)
			ctx := context.Background()
			servers := make([]ConflictResolver, 2)
			for i := range servers {
				r, err := NewConflictResolver(mode, rdb, fmt.Sprintf("s%d", i+1))
				if err != nil {
					t.Fatal(err)
				}
				servers[i] = r
			}

			var mu sync.Mutex
			var broadcast []PixelEvent
			var wg sync.WaitGroup
			for w := 0; w < 8; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					srv := servers[w%len(servers)]
					for i := 0; i < 50; i++ {
						ev := px(i%3, 0, fmt.Sprintf("#%02x%02x00", w, i))
						out, err := srv.Apply(ctx, ev)
						if errors.Is(err, ErrStaleWrite) {
							continue
						}
						if err != nil {
							t.Error(err)
							return
						}
						mu.Lock()
						broadcast = append(broadcast, out)
						mu.Unlock()
						raw, _ := json.Marshal(pixelMessage{Type: "pixel", PixelEvent: out})
						for _, s := range servers {
							s.Observe(raw)
						}
					}
				}(w)
			}
			wg.Wait()

			rng := rand.New(rand.NewSource(1))
			for c := 0; c < 20; c++ {
				cl := &client{pixels: map[[2]int]PixelEvent{}}
				order := rng.Perm(len(broadcast))
				for _, i := range order {
					cl.receive(broadcast[i])
				}
				for x := 0; x < 3; x++ {
					want := pixelColor(t, canvas, x, 0)
					if got := cl.pixels[[2]int{x, 0}].Color; got != want {
						t.Fatalf("client %d pixel (%d,0) = %s, Redis has %s", c, x, got, want)
					}
				}
			}
		})
	}
}
