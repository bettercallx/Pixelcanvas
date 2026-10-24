/*
HTTP endpoints
GET  /api/canvas         new client connect → return canvas state
GET  /api/canvas/config 	return canvas config (width, height)
GET  /ws				  websocket upgrade

new websocket connection → register client → read loop (receive PixelEvent) → canvas.SetPixel → hub.BroadcastEvent
*/
package main

// WebSocket handler:
//func HandleWebSocket(hub *Hub, canvas *Canvas, w http.ResponseWriter, r *http.Request)
