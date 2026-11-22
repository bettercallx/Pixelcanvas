// HTTP endpoints
// GET  /api/chunks          list open chunks             -> [{"cx":0,"cy":0}, ...]
// GET  /api/chunk/:cx/:cy   raw RGB bytes of one chunk   -> application/octet-stream, chunkBytes long
// POST /api/chunks          open a chunk {"cx":1,"cy":0} -> 201 / 403 not allowed / 409 already open / 507 max chunks
// GET  /ws                  websocket upgrade
//
// WebSocket messages (JSON, always with "type")
// browser -> server: {"type":"pixel","x":10,"y":-3,"color":"#ff8800"}
// server -> browser: {"type":"pixel","x":10,"y":-3,"color":"#ff8800","user_id":"...","timestamp":...}
//                    {"type":"chunk_added","cx":1,"cy":0}
//                    {"type":"error","message":"chunk is not open"}
//
// pixel flow: readPump -> handleMessage -> canvas.SetPixel -> broadcaster.Publish
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

// Server wires the modules together; handlers are its methods
type Server struct {
	canvas *Canvas
	policy ChunkPolicy
	hub    *Hub
	bc     Broadcaster
}

func (s *Server) Routes(r *gin.Engine) {
	r.GET("/api/chunks", s.listChunks)
	r.GET("/api/chunk/:cx/:cy", s.getChunk)
	r.POST("/api/chunks", s.addChunk)
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
	data, err := s.canvas.GetChunk(c.Request.Context(), ChunkCoord{cx, cy})
	if errors.Is(err, ErrChunkNotOpen) {
		c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		return
	}
	if err != nil {
		internalError(c, err)
		return
	}
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

		ctx, cancel := context.WithTimeout(context.Background(), redisTimeout)
		defer cancel()

		if err := s.canvas.SetPixel(ctx, ev); err != nil {
			if errors.Is(err, ErrChunkNotOpen) || errors.Is(err, ErrInvalidColor) {
				s.sendError(c, err.Error())
			} else {
				log.Printf("SetPixel from %s: %v", c.ID, err)
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
