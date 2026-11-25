package main

import (
	"context"
	"errors"

	"github.com/redis/go-redis/v9"
)

const (
	activeVersionKey  = "canvas:active_version"
	versionCounterKey = "canvas:version_counter"
	initialVersion    = "1"
)

var ErrCanvasVersionNotInitialized = errors.New(
	"canvas active version is not initialized",
)

func versionChunksKey(version string) string {
	return "canvas:v" + version + ":chunks"
}

func versionChunkKey(version string, cc ChunkCoord) string {
	return "canvas:v" + version + ":chunk:" + cc.member()
}

func versionClockKey(version string, cc ChunkCoord) string {
	return "canvas:v" + version + ":clock:" + cc.member()
}

// activeVersion returns the version currently published to canvas readers.
func activeVersion(ctx context.Context, rdb *redis.Client) (string, error) {
	version, err := rdb.Get(ctx, activeVersionKey).Result()
	if errors.Is(err, redis.Nil) {
		return "", ErrCanvasVersionNotInitialized
	}
	return version, err
}

func initializeCanvasVersion(ctx context.Context, rdb *redis.Client) error {
	_, err := initializeCanvasVersionScript.Run(
		ctx,
		rdb,
		[]string{activeVersionKey, chunksSetKey, versionCounterKey},
		initialVersion,
	).Text()
	return err
}

var isOpenVersionScript = redis.NewScript(`
local version = redis.call('GET', KEYS[1])
if not version then
  return redis.error_reply('canvas active version is not initialized')
end

local key = 'canvas:v' .. version .. ':chunks'
return redis.call('SISMEMBER', key, ARGV[1])
`)

var getChunkVersionScript = redis.NewScript(`
local version = redis.call('GET', KEYS[1])
if not version then
  return redis.error_reply('canvas active version is not initialized')
end

local key = 'canvas:v' .. version .. ':chunk:' .. ARGV[1]
local data = redis.call('GET', key)
if not data then
  return false
end
return data
`)

var getChunkWithSeqVersionScript = redis.NewScript(`
local version = redis.call('GET', KEYS[1])
if not version then
  return redis.error_reply('canvas active version is not initialized')
end

local key = 'canvas:v' .. version .. ':chunk:' .. ARGV[1]
local data = redis.call('GET', key)
if not data then
  return {0}
end

local seq = redis.call('GET', KEYS[2]) or '0'
return {1, data, seq}
`)

var listChunksVersionScript = redis.NewScript(`
local version = redis.call('GET', KEYS[1])
if not version then
  return redis.error_reply('canvas active version is not initialized')
end

local key = 'canvas:v' .. version .. ':chunks'
return redis.call('SMEMBERS', key)
`)

// cloneActiveCanvasVersion creates a complete copy of the active canvas
// under a new version and returns the new version number.
//
// Redis executes the script atomically, so readers and writers cannot
// observe a partially copied version.
func cloneActiveCanvasVersion(ctx context.Context, rdb *redis.Client) (string, error) {
	return cloneActiveCanvasVersionScript.Run(
		ctx,
		rdb,
		[]string{activeVersionKey, versionCounterKey},
	).Text()
}

var cloneActiveCanvasVersionScript = redis.NewScript(`
local active = redis.call('GET', KEYS[1])
if not active then
  return redis.error_reply('canvas active version is not initialized')
end

local sourceChunksKey = 'canvas:v' .. active .. ':chunks'
if redis.call('EXISTS', sourceChunksKey) == 0 then
  return redis.error_reply('active canvas chunk set is missing')
end

local members = redis.call('SMEMBERS', sourceChunksKey)

-- Allocate a version that does not already contain data.
local newVersion = nil
local newChunksKey = nil

for attempt = 1, 100 do
  local candidate = tostring(redis.call('INCR', KEYS[2]))
  local candidateChunksKey = 'canvas:v' .. candidate .. ':chunks'
  local collision = redis.call('EXISTS', candidateChunksKey) == 1

  for _, member in ipairs(members) do
    local chunkKey = 'canvas:v' .. candidate .. ':chunk:' .. member
    local clockKey = 'canvas:v' .. candidate .. ':clock:' .. member

    if redis.call('EXISTS', chunkKey) == 1 or
       redis.call('EXISTS', clockKey) == 1 then
      collision = true
    end
  end

  if not collision then
    newVersion = candidate
    newChunksKey = candidateChunksKey
    break
  end
end

if not newVersion then
  return redis.error_reply('could not allocate an unused canvas version')
end

-- Read and validate all source data before creating the new version.
local chunks = {}
local clocks = {}

for _, member in ipairs(members) do
  local sourceChunkKey = 'canvas:v' .. active .. ':chunk:' .. member
  local sourceClockKey = 'canvas:v' .. active .. ':clock:' .. member

  local chunkData = redis.call('GET', sourceChunkKey)
  if not chunkData then
    return redis.error_reply('active canvas chunk is missing: ' .. member)
  end

  chunks[member] = chunkData
  clocks[member] = redis.call('HGETALL', sourceClockKey)
end

-- Copy the open-chunk set, chunk data, and clock hashes.
for _, member in ipairs(members) do
  redis.call('SADD', newChunksKey, member)

  local chunkKey = 'canvas:v' .. newVersion .. ':chunk:' .. member
  redis.call('SET', chunkKey, chunks[member])

  local clockFields = clocks[member]
  if #clockFields > 0 then
    local clockKey = 'canvas:v' .. newVersion .. ':clock:' .. member

    for i = 1, #clockFields, 2 do
      redis.call('HSET', clockKey, clockFields[i], clockFields[i + 1])
    end
  end
end

return newVersion
`)

var initializeCanvasVersionScript = redis.NewScript(`
local activeKey = KEYS[1]
local legacyChunksKey = KEYS[2]
local version = ARGV[1]

local active = redis.call('GET', activeKey)
if active then
  local counter = redis.call('GET', KEYS[3])
  if not counter or tonumber(counter) < tonumber(active) then
    redis.call('SET', KEYS[3], active)
  end
  return active
end

local newChunksKey = 'canvas:v' .. version .. ':chunks'

-- Do not overwrite leftover versioned data if the active pointer is missing.
if redis.call('EXISTS', newChunksKey) == 1 then
  return redis.error_reply('versioned chunks exist but active version is missing')
end

local members = redis.call('SMEMBERS', legacyChunksKey)

-- Validate all source and destination keys before changing anything.
for _, member in ipairs(members) do
  local oldChunk = 'canvas:chunk:' .. member
  local newChunk = 'canvas:v' .. version .. ':chunk:' .. member
  local oldClock = 'canvas:clock:' .. member
  local newClock = 'canvas:v' .. version .. ':clock:' .. member

  if redis.call('EXISTS', oldChunk) == 0 then
    return redis.error_reply('legacy chunk is missing: ' .. oldChunk)
  end

  if redis.call('EXISTS', newChunk) == 1 or redis.call('EXISTS', newClock) == 1 then
    return redis.error_reply('versioned chunk or clock already exists')
  end
end

if #members == 0 then
  -- Fresh canvas: initialize the origin chunk as white.
  redis.call('SET', 'canvas:v' .. version .. ':chunk:0:0',
    string.rep(string.char(255), 64 * 64 * 3))
  redis.call('SADD', newChunksKey, '0:0')
else
  -- Move the existing open-chunk set and chunk data into version 1.
  redis.call('RENAME', legacyChunksKey, newChunksKey)

  for _, member in ipairs(members) do
    local oldChunk = 'canvas:chunk:' .. member
    local newChunk = 'canvas:v' .. version .. ':chunk:' .. member
    local oldClock = 'canvas:clock:' .. member
    local newClock = 'canvas:v' .. version .. ':clock:' .. member

    redis.call('RENAME', oldChunk, newChunk)

    if redis.call('EXISTS', oldClock) == 1 then
      redis.call('RENAME', oldClock, newClock)
    end
  end
end

redis.call('SET', KEYS[3], version)
redis.call('SET', activeKey, version)
return version
`)
