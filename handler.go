// HTTP endpoints
// GET  /api/chunks          list open chunks             -> [{"cx":0,"cy":0}, ...]
// GET  /api/chunk/:cx/:cy   raw RGB bytes of one chunk   -> application/octet-stream, chunkBytes long
//                           header X-Canvas-Seq: global seq at read time
// POST /api/chunks          open a chunk {"cx":1,"cy":0} -> 201 / 403 not allowed / 409 already open / 507 max chunks
// GET  /ws                  websocket upgrade
//
// WebSocket messages (JSON, always with "type")
// browser -> server: {"type":"pixel","x":10,"y":-3,"color":"#ff8800"}
// server -> browser: {"type":"pixel","x":10,"y":-3,"color":"#ff8800","user_id":"...","timestamp":...,"seq":42}
//                    (lww mode sends "lamport" and "server_id" instead of "seq"; apply only if Supersedes)
//                    {"type":"chunk_added","cx":1,"cy":0}
//                    {"type":"error","message":"chunk is not open"}
//
// pixel flow: readPump -> handleMessage -> resolver.Apply -> broadcaster.Publish
//             -> Redis -> every server's subscriber -> hub.Broadcast -> writePump

package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
)

const redisTimeout = 2 * time.Second // per-message budget for Redis calls

// embedding PixelEvent flattens its fields into the same JSON object
type pixelMessage struct {
	Type string `json:"type"`
	PixelEvent
}

type chunkAddedMessage struct {
	Type string `json:"type"`
	ChunkCoord
}

type errorMessage struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

type rollbackMessage struct {
	Type     string `json:"type"`
	TargetID string `json:"target_id"`
}

// Server wires the modules together; handlers are its methods
type Server struct {
	canvas   *Canvas
	resolver ConflictResolver
	policy   ChunkPolicy
	hub      *Hub
	bc       Broadcaster
	rollback *ReverseUndoRollback
}

func (s *Server) Routes(r *gin.Engine) {
	r.GET("/api/chunks", s.listChunks)
	r.GET("/api/chunk/:cx/:cy", s.getChunk)
	r.POST("/api/chunks", s.addChunk)
	r.POST("/api/rollback", s.rollbackCanvas)
	r.GET("/ws", s.serveWS)
}

func (s *Server) listChunks(c *gin.Context) {
	coords, err := s.canvas.ListChunks(c.Request.Context())
	if err != nil {
		internalError(c, err)
		return
	}
	c.JSON(http.StatusOK, coords)
}

func (s *Server) getChunk(c *gin.Context) {
	cx, errX := strconv.Atoi(c.Param("cx"))
	cy, errY := strconv.Atoi(c.Param("cy"))
	if errX != nil || errY != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "cx and cy must be integers"})
		return
	}
	data, seq, err := s.canvas.GetChunkWithSeq(c.Request.Context(), ChunkCoord{cx, cy})
	if errors.Is(err, ErrChunkNotOpen) {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	if err != nil {
		internalError(c, err)
		return
	}
	c.Header("X-Canvas-Seq", strconv.FormatInt(seq, 10))
	c.Data(http.StatusOK, "application/octet-stream", data)
}

func (s *Server) addChunk(c *gin.Context) {
	var cc ChunkCoord
	if err := c.ShouldBindJSON(&cc); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "body must be {\"cx\":int,\"cy\":int}"})
		return
	}
	ctx := c.Request.Context()

	ok, err := s.policy.CanAdd(ctx, cc)
	if err != nil {
		internalError(c, err)
		return
	}
	if !ok {
		c.JSON(http.StatusForbidden, gin.H{"error": "chunk must be next to an open chunk"})
		return
	}

	added, err := s.canvas.AddChunk(ctx, cc)
	if errors.Is(err, ErrMaxChunks) {
		c.JSON(http.StatusInsufficientStorage, gin.H{"error": err.Error()})
		return
	}
	if err != nil {
		internalError(c, err)
		return
	}
	if !added {
		c.JSON(http.StatusConflict, gin.H{"error": "chunk is already open"})
		return
	}

	msg, _ := json.Marshal(chunkAddedMessage{Type: "chunk_added", ChunkCoord: cc})
	if err := s.bc.Publish(ctx, cc, msg); err != nil {
		// the chunk is open in Redis; other clients will see it on their next reload
		log.Printf("publish chunk_added %v: %v", cc, err)
	}
	c.JSON(http.StatusCreated, cc)
}

func (s *Server) rollbackCanvas(c *gin.Context) {
	var req struct {
		TargetID string `json:"target_id"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "body must contain a target_id"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Minute)
	defer cancel()

	if err := s.rollback.Rollback(ctx, req.TargetID); err != nil {
		if errors.Is(err, ErrInvalidRollbackTarget) {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		if errors.Is(err, ErrUnsupportedEvent) {
			c.JSON(http.StatusUnprocessableEntity, gin.H{"error": err.Error()})
			return
		}

		log.Printf("rollback to %s failed: %v", req.TargetID, err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "rollback failed"})
		return
	}

	// The rollback has committed. Notify every application server.
	msg, err := json.Marshal(rollbackMessage{
		Type:     "rollback",
		TargetID: req.TargetID,
	})
	if err != nil {
		log.Printf("marshal rollback notification: %v", err)
	} else {
		publishCtx, publishCancel := context.WithTimeout(context.Background(), redisTimeout)
		defer publishCancel()

		if err := s.bc.Publish(publishCtx, ChunkCoord{}, msg); err != nil {
			log.Printf("publish rollback notification: %v", err)
		}
	}

	c.JSON(http.StatusOK, gin.H{
		"status":    "rolled_back",
		"target_id": req.TargetID,
	})
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

func (s *Server) serveWS(c *gin.Context) {
	conn, err := upgrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		return // Upgrade already wrote the HTTP error response
	}
	client := NewClient(newClientID(), conn, s.hub)
	s.hub.Register <- client
	go client.writePump()
	go client.readPump(s.handleMessage)
}

// handleMessage runs on the client's readPump goroutine, once per incoming message
func (s *Server) handleMessage(c *Client, raw []byte) {
	var msg pixelMessage
	if err := json.Unmarshal(raw, &msg); err != nil {
		s.sendError(c, "invalid JSON")
		return
	}

	switch msg.Type {
	case "pixel":
		ev := msg.PixelEvent
		ev.UserID = c.ID
		ev.Timestamp = time.Now().UnixMilli()
		ev.Seq, ev.Lamport, ev.ServerID = 0, 0, "" // never trust stamps from the browser

		ctx, cancel := context.WithTimeout(context.Background(), redisTimeout)
		defer cancel()

		ev, err := s.resolver.Apply(ctx, ev)
		if err != nil {
			switch {
			case errors.Is(err, ErrStaleWrite):
				// the winning write is already being broadcast
			case errors.Is(err, ErrChunkNotOpen) ||
				errors.Is(err, ErrInvalidColor) ||
				errors.Is(err, ErrRollbackInProgress):
				s.sendError(c, err.Error())
			default:
				log.Printf("Apply from %s: %v", c.ID, err)
				s.sendError(c, "server error")
			}
			return
		}

		out, _ := json.Marshal(pixelMessage{Type: "pixel", PixelEvent: ev})
		cc, _ := locate(ev.X, ev.Y)
		if err := s.bc.Publish(ctx, cc, out); err != nil {
			// pixel is saved; other clients will get it on their next reload
			log.Printf("publish pixel: %v", err)
		}

	default:
		s.sendError(c, "unknown message type "+strconv.Quote(msg.Type))
	}
}

func (s *Server) sendError(c *Client, text string) {
	msg, _ := json.Marshal(errorMessage{Type: "error", Message: text})
	s.hub.SendTo(c, msg)
}

// log the real error, show the browser a generic one
func internalError(c *gin.Context, err error) {
	log.Printf("%s %s: %v", c.Request.Method, c.Request.URL.Path, err)
	c.JSON(http.StatusInternalServerError, gin.H{"error": "server error"})
}

// random 16-hex-char id per connection
func newClientID() string {
	b := make([]byte, 8)
	rand.Read(b)
	return hex.EncodeToString(b)
}
