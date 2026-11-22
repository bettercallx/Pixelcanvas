# Pixelcanvas

A distributed collaborative pixel canvas

## Architecture

![System Architecture](Assets/architecture.png)

The canvas is infinite, split into 64×64 chunks. Each chunk is one Redis key (3 bytes RGB per pixel).

### Pixel flow

```
browser --WebSocket--> server (handleMessage)
  -> ConflictResolver: Lua script in Redis, atomic: check chunk open, stamp/compare order, write, log to canvas events
  -> Publish pixels
  -> every subscriber -> hub -> its browsers
```

## Run locally

```
docker run -d --name redis -p 6379:6379 redis:7
go run .                          # :8080
PORT=8081 SERVER_ID=s2 go run .   # second server
go test ./...                     # uses in-memory miniredis
TEST_REDIS_ADDR=localhost:6390 go test ./...   # same tests on a real, throwaway Redis (they FLUSHDB it)
```

Config is via env vars: `PORT`, `SERVER_ID`, `REDIS_ADDR`, `MAX_CHUNKS`, `CHUNK_POLICY` (adjacent|free), `BROADCAST_MODE` (global), `CONFLICT_MODE` (sequencer|lww).

## Conflict resolution

Two servers can write the same pixel at once. Redis applies them in one order, but each server publishes its own event, so clients can get the two messages in the opposite order. Every pixel event carries an ordering stamp, and **clients keep a pixel event only if it supersedes the one they have** (`Supersedes` in `conflict.go`):

| `CONFLICT_MODE` | stamp | rule |
|---|---|---|
| `sequencer` (default) | `seq`: Redis `INCR canvas:seq` inside the write script | bigger `seq` wins |
| `lww` | `lamport` + `server_id`: each server's Lamport clock, advanced on every event it hears over Pub/Sub | bigger `(lamport, server_id)` wins; Redis also refuses to store a write whose stamp is not bigger than the pixel's stored one (`canvas:clock:cx:cy`) |

- Sequencer: one central place decides a total order; clients converge to exactly Redis' apply order.
- LWW: the result doesn't depend on the order writes arrive (a per-pixel LWW register, a simple CRDT). A write that arrives with an older stamp than the stored one is dropped (no broadcast, no log entry). `SERVER_ID` must be unique per server in this mode.
- `GET /api/chunk/:cx/:cy` returns `X-Canvas-Seq`. In sequencer mode, a client that loads a chunk should ignore pixel events with `seq` at or below that number, because they're already in the bytes.
- Rollback in lww mode: pixels it restores need fresh stamps, bigger than the ones stored, or the restore writes will be dropped.

## API

| | |
|---|---|
| `GET /api/chunks` | open chunks `[{"cx":0,"cy":0}]` |
| `GET /api/chunk/:cx/:cy` | raw RGB bytes of one chunk |
| `POST /api/chunks` `{"cx":1,"cy":0}` | open a chunk (201 / 403 / 409 / 507) |
| `GET /ws` | WebSocket |

WebSocket messages are JSON with a `type`:
- send: `{"type":"pixel","x":10,"y":-3,"color":"#ff8800"}`
- receive: `pixel` (same fields + `user_id`, `timestamp`, and `seq` or `lamport`/`server_id`), `chunk_added`, `error`

## Adding rollback

Every change is in the Redis Stream `canvas:events`:

```
<id>  type=pixel x=10 y=10 color=#0000ff prev_color=#ff0000 user_id=... timestamp=... seq=42   (or lamport=17 server_id=s1)
<id>  type=chunk_added cx=0 cy=1
```

Read it with `NewEventLog(rdb).Range(...)` / `.RevRange(...)` in `eventlog.go`.

The stream ID is the global sequence number. Order by it, not by `timestamp`.

Each entry has both `color` and `prev_color`, so either rollback style works:
- Replay (planned): start from the nearest snapshot and apply `color` forward up to the target ID. Snapshots are not built yet.
- Reverse undo: walk newest first and set each pixel back to `prev_color`.

## Tech Stack
- Go + Gin
- WebSocket (gorilla/websocket)
- Redis (state + Pub/Sub + Streams)
- Nginx (load balancer)

## Team
- Xiang Li
- Alexandra Descoteaux
- William Beeker
