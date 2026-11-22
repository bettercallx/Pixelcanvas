package main

import (
	"context"
	"log"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func main() {
	cfg := LoadConfig()
	ctx := context.Background()

	// fail fast if Redis is unreachable
	rdb := redis.NewClient(&redis.Options{Addr: cfg.RedisAddr})
	if err := rdb.Ping(ctx).Err(); err != nil {
		log.Fatalf("connect redis %s: %v", cfg.RedisAddr, err)
	}

	canvas, err := NewCanvas(ctx, rdb, cfg.MaxChunks)
	if err != nil {
		log.Fatal(err)
	}
	policy, err := NewChunkPolicy(cfg.ChunkPolicy, canvas)
	if err != nil {
		log.Fatal(err)
	}

	hub := NewHub()
	go hub.Run()

	bc, err := NewBroadcaster(cfg.BroadcastMode, rdb, hub)
	if err != nil {
		log.Fatal(err)
	}
	if err := bc.Start(ctx); err != nil {
		log.Fatal(err)
	}

	srv := &Server{canvas: canvas, policy: policy, hub: hub, bc: bc}
	r := gin.Default()
	srv.Routes(r)

	log.Printf("server %s listening on :%s (redis=%s chunk_policy=%s broadcast=%s max_chunks=%d)",
		cfg.ServerID, cfg.Port, cfg.RedisAddr, cfg.ChunkPolicy, cfg.BroadcastMode, cfg.MaxChunks)
	if err := r.Run(":" + cfg.Port); err != nil {
		log.Fatal(err)
	}
}
