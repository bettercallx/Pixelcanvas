package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
)

func TestRollbackBroadcastReachesAllWebSocketClients(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rdb, canvas, _, rollback := newTestRollback(t)

	resolver, err := NewConflictResolver("sequencer", rdb, "")
	if err != nil {
		t.Fatalf("NewConflictResolver: %v", err)
	}

	// Start the Hub and Redis broadcaster.
	hub := NewHub()
	go hub.Run()

	bc, err := NewBroadcaster("global", rdb, hub, resolver.Observe)
	if err != nil {
		t.Fatalf("NewBroadcaster: %v", err)
	}
	if err := bc.Start(ctx); err != nil {
		t.Fatalf("Broadcaster Start: %v", err)
	}

	// Build the HTTP server.
	srv := &Server{
		canvas:   canvas,
		resolver: resolver,
		hub:      hub,
		bc:       bc,
		rollback: rollback,
	}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	srv.Routes(router)

	httpServer := httptest.NewServer(router)
	defer httpServer.Close()

	wsURL := "ws" + strings.TrimPrefix(httpServer.URL, "http") + "/ws"

	// Connect two independent WebSocket clients.
	client1, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("connect client 1: %v", err)
	}
	defer client1.Close()

	client2, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("connect client 2: %v", err)
	}
	defer client2.Close()

	// Set read deadlines for both clients.
	if err := client1.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := client2.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}

	// Send a readiness message and verify both clients receive it.
	readyMsg := []byte(`{"type":"test_ready"}`)
	if err := bc.Publish(ctx, ChunkCoord{}, readyMsg); err != nil {
		t.Fatalf("publish readiness message: %v", err)
	}

	for i, client := range []*websocket.Conn{client1, client2} {
		_, msg, err := client.ReadMessage()
		if err != nil {
			t.Fatalf("client %d readiness message: %v", i+1, err)
		}
		if string(msg) != string(readyMsg) {
			t.Fatalf("client %d received %s; want %s", i+1, msg, readyMsg)
		}
	}

	// Create a checkpoint.
	checkpoint, err := rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: eventStreamKey,
		Values: map[string]interface{}{
			"type": "checkpoint",
		},
	}).Result()
	if err != nil {
		t.Fatalf("create checkpoint: %v", err)
	}

	// Make a pixel change after the checkpoint.
	_, err = resolver.Apply(ctx, PixelEvent{
		X:         10,
		Y:         10,
		Color:     "#ff0000",
		UserID:    "broadcast-test",
		Timestamp: 123456789,
	})
	if err != nil {
		t.Fatalf("Apply pixel: %v", err)
	}

	// Trigger rollback through the actual HTTP endpoint.
	body, err := json.Marshal(map[string]string{
		"target_id": checkpoint,
	})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := http.Post(
		httpServer.URL+"/api/rollback",
		"application/json",
		bytes.NewReader(body),
	)
	if err != nil {
		t.Fatalf("POST /api/rollback: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("rollback HTTP status = %d; want %d", resp.StatusCode, http.StatusOK)
	}

	// Verify that both clients receive the rollback notification.
	for i, client := range []*websocket.Conn{client1, client2} {
		_, msg, err := client.ReadMessage()
		if err != nil {
			t.Fatalf("client %d rollback notification: %v", i+1, err)
		}

		var notification rollbackMessage
		if err := json.Unmarshal(msg, &notification); err != nil {
			t.Fatalf("client %d invalid JSON: %v", i+1, err)
		}

		if notification.Type != "rollback" {
			t.Errorf("client %d message type = %q; want rollback",
				i+1, notification.Type)
		}

		if notification.TargetID != checkpoint {
			t.Errorf("client %d target ID = %q; want %q",
				i+1, notification.TargetID, checkpoint)
		}
	}

	// Confirm that rollback actually changed the canvas.
	data, err := canvas.GetChunk(ctx, ChunkCoord{CX: 0, CY: 0})
	if err != nil {
		t.Fatalf("GetChunk after rollback: %v", err)
	}

	_, offset := locate(10, 10)
	if got := data[offset : offset+3]; string(got) != string([]byte{255, 255, 255}) {
		t.Fatalf("pixel after rollback = %v; want white", got)
	}
}
