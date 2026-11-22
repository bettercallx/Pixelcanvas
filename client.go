// one read goroutine + one write goroutine per websocket
package main

import (
	"log"
	"time"

	"github.com/gorilla/websocket"
)

const (
	writeWait      = 10 * time.Second  // write one message max wait 10s
	pongWait       = 60 * time.Second  // no message from the browser for 60s means the connection is dead
	pingPeriod     = pongWait * 9 / 10 // 54s send pings
	maxMessageSize = 512               // a pixel event is ~100 bytes of JSON, exceed this client is misbehaving
	sendBufferSize = 256               // queued outgoing messages
)

type Client struct {
	ID   string
	Conn *websocket.Conn
	Send chan []byte // outgoing messages; only writePump writes to Conn
	hub  *Hub
}

func NewClient(id string, conn *websocket.Conn, hub *Hub) *Client {
	return &Client{
		ID:   id,
		Conn: conn,
		Send: make(chan []byte, sendBufferSize),
		hub:  hub,
	}
}

// readPump reads messages from the browser and passes each one to handle.
// When it returns browser left or timed out, the client is unregistered.
func (c *Client) readPump(handle func(c *Client, msg []byte)) {
	defer func() {
		c.hub.Unregister <- c
		c.Conn.Close()
	}()

	c.Conn.SetReadLimit(maxMessageSize)
	c.Conn.SetReadDeadline(time.Now().Add(pongWait))
	// every pong pushes the deadline forward
	c.Conn.SetPongHandler(func(string) error {
		return c.Conn.SetReadDeadline(time.Now().Add(pongWait))
	})

	for {
		_, msg, err := c.Conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("client %s read error: %v", c.ID, err)
			}
			return
		}
		handle(c, msg)
	}
}

// writePump is the only goroutine that writes to Conn: queued messages and pings
func (c *Client) writePump() {
	ticker := time.NewTicker(pingPeriod)
	defer func() {
		ticker.Stop()
		c.Conn.Close()
	}()

	for {
		select {
		case msg, ok := <-c.Send: // got a message to send
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if !ok {
				c.Conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}
			if err := c.Conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C: //need to send a ping
			c.Conn.SetWriteDeadline(time.Now().Add(writeWait))
			if err := c.Conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}
