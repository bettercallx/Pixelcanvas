// single pixel
type PixelEvent struct {
	X         int    `json:"x"`
	Y         int    `json:"y"`
	Color     string `json:"color"`
	UserID    string `json:"user_id"`
	Timestamp int64  `json:"timestamp"`
}

// canvas state
type Canvas struct {
	Width  int
	Height int
	Pixels [][]string // Pixels[y][x] = color hex
	mu     sync.RWMutex
}

// client connection
type Client struct {
	ID   string
	Conn *websocket.Conn
	Send chan []byte
}

// hub to manage all connections + broadcast
type Hub struct {
	Clients    map[string]*Client
	Broadcast  chan []byte
	Register   chan *Client
	Unregister chan *Client
	mu         sync.RWMutex
}

/*HTTP endpoints
GET  /api/canvas         new client connect → return canvas state
GET  /api/canvas/config 	return canvas config (width, height)
GET  /ws				  websocket upgrade

new websocket connection → register client → read loop (receive PixelEvent) → canvas.SetPixel → hub.BroadcastEvent
*/

// canvas:
func NewCanvas(width, height int) *Canvas
func (c *Canvas) GetPixel(x, y int) string
func (c *Canvas) SetPixel(event PixelEvent) error
func (c *Canvas) Snapshot() [][]string // return a copy of current canvas state

// Hub:
func NewHub() *Hub
func (h *Hub) Run()                            // main loop: handle register/unregister/broadcast
func (h *Hub) BroadcastEvent(event PixelEvent) // serialize event and broadcast to all clients

// WebSocket handler:
func HandleWebSocket(hub *Hub, canvas *Canvas, w http.ResponseWriter, r *http.Request)
