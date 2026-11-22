// manage all connections on this server + broadcast
// c.Send is only written to and closed inside Run(), so a send can never hit a closed channel
package main

type Hub struct {
	clients    map[*Client]struct{}
	Broadcast  chan []byte // messages to send to every client on this server
	Register   chan *Client
	Unregister chan *Client
	direct     chan directMessage // messages for a single client, see SendTo
}

type directMessage struct {
	client *Client
	msg    []byte
}

func NewHub() *Hub {
	return &Hub{
		clients:    make(map[*Client]struct{}),
		Broadcast:  make(chan []byte, 256),
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		direct:     make(chan directMessage, 64),
	}
}

func (h *Hub) Run() {
	for {
		select {
		case c := <-h.Register:
			h.clients[c] = struct{}{}

		case c := <-h.Unregister:
			// a slow client may already be removed
			if _, ok := h.clients[c]; ok {
				h.remove(c)
			}

		case msg := <-h.Broadcast:
			for c := range h.clients {
				h.deliver(c, msg)
			}

		case d := <-h.direct:
			// the client may have left while the message was queued
			if _, ok := h.clients[d.client]; ok {
				h.deliver(d.client, d.msg)
			}
		}
	}
}

// SendTo queues a message for one client (e.g. an error reply).
// Never write to c.Send directly from outside the hub: it may already be closed.
func (h *Hub) SendTo(c *Client, msg []byte) {
	h.direct <- directMessage{client: c, msg: msg}
}

// if the client's buffer is full it is too slow, drop it instead of blocking everyone
func (h *Hub) deliver(c *Client, msg []byte) {
	select {
	case c.Send <- msg:
	default:
		h.remove(c)
	}
}

// deletes the client and closes Send, making the writePump exit
func (h *Hub) remove(c *Client) {
	delete(h.clients, c)
	close(c.Send)
}
