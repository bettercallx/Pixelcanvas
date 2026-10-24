// hub to manage all connections + broadcast
package main

import "sync"

type Hub struct {
	Clients    map[string]*Client
	Broadcast  chan []byte
	Register   chan *Client
	Unregister chan *Client
	Redis      *redis.Client
	mu         sync.RWMutex
}

// func NewHub() *Hub
// func (h *Hub) Run()                            // main loop: handle register/unregister/broadcast
// func (h *Hub) BroadcastEvent(event PixelEvent) // serialize event and broadcast to all clients
