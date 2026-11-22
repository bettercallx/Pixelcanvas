// server config, loaded from environment variables
package main

import (
	"os"
	"strconv"
)

type Config struct {
	Port        string // HTTP port this server listens on
	ServerID    string // identifies this instance in logs/metrics (e.g. "server1")
	RedisAddr   string // host:port of Redis
	MaxChunks   int    // cap on opened chunks, keeps Redis memory bounded (~12KB per chunk)
	ChunkPolicy string // where new chunks may be added: "adjacent" | "free" (validated in NewChunkPolicy)
}

func LoadConfig() Config {
	host, _ := os.Hostname()
	return Config{
		Port:        getEnv("PORT", "8080"),
		ServerID:    getEnv("SERVER_ID", host),
		RedisAddr:   getEnv("REDIS_ADDR", "localhost:6379"),
		MaxChunks:   getEnvInt("MAX_CHUNKS", 400),
		ChunkPolicy: getEnv("CHUNK_POLICY", "adjacent"),
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
