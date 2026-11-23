// server config, loaded from environment variables
package main

import (
	"os"
	"strconv"
)

type Config struct {
	Port          string // server listen on 8080
	ServerID      string // identifies this instance in logs/metrics
	RedisAddr     string // host:port of Redis
	MaxChunks     int    // cap on opened chunks, keeps Redis memory bounded (~12KB per chunk)
	ChunkPolicy   string // new chunks added mode: adjacent or free
	BroadcastMode string // cross-server broadcast mode: global (sharded in phase 2)
	ConflictMode  string // sequencer or lww
}

// default server config, can be overridden through docker `PORT=9090 go run .`
func LoadConfig() Config {
	host, _ := os.Hostname()
	return Config{
		Port:          getEnv("PORT", "8080"),
		ServerID:      getEnv("SERVER_ID", host),
		RedisAddr:     getEnv("REDIS_ADDR", "localhost:6379"),
		MaxChunks:     getEnvInt("MAX_CHUNKS", 400),
		ChunkPolicy:   getEnv("CHUNK_POLICY", "adjacent"),
		BroadcastMode: getEnv("BROADCAST_MODE", "global"),
		ConflictMode:  getEnv("CONFLICT_MODE", "sequencer"),
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil && v > 0 {
		return v
	}
	return fallback
}
