package hub

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"tcp-chat/internal/domain"
	"time"
	"uuid"
)

type Hub struct {
	clients    map[string]*domain.Client
	broadcast  chan domain.ChatMessage
	register   chan *domain.Client
	unregister chan *domain.Client
	req        chan Request
	history    MessageHistory
	Logger     *log.Logger
	Stats      *domain.ServerStats
	Wg         sync.WaitGroup
}

type Request struct {
	ActiveUserResponse chan []string
	CountUserResponse  chan int
}

type MessageHistory struct {
	buf  []domain.ChatMessage
	head int
}

func NewHub(logger *log.Logger, historySize int) *Hub {
	return &Hub{
		clients:    make(map[string]*domain.Client),
		broadcast:  make(chan domain.ChatMessage),
		register:   make(chan *domain.Client),
		unregister: make(chan *domain.Client),
		req:        make(chan Request),
		history:    MessageHistory{buf: make([]domain.ChatMessage, historySize), head: 0},
		Logger:     logger,
		Stats:      &domain.ServerStats{StartedAt: time.Now().Unix()},
	}
}

func (h *Hub) GetActiveClients() []string {
	response := make(chan []string)
	h.req <- Request{
		ActiveUserResponse: response,
	}
	return <-response
}

func (h *Hub) GetClientCount() int {
	response := make(chan int)
	h.req <- Request{
		CountUserResponse: response,
	}

	return <-response
}

func (h *Hub) Run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case client := <-h.register:
			h.clients[client.ID] = client
			h.Stats.ActiveConnections++
		case client := <-h.unregister:
			if _, ok := h.clients[client.ID]; ok {
				delete(h.clients, client.ID)
				h.Stats.ActiveConnections--
			}
		case message, ok := <-h.broadcast:
			if !ok {
				return
			}
			h.BroadcastMessage(message)
			h.history.Add(message)
			h.Stats.TotalMessagesProcessed++
		case req := <-h.req:
			if req.ActiveUserResponse != nil {
				clients := make([]string, 0)
				for k := range h.clients {
					clients = append(clients, k)
				}

				req.ActiveUserResponse <- clients
			} else if req.CountUserResponse != nil {
				req.CountUserResponse <- len(h.clients)
			}
		}
	}
}

func (h *Hub) BroadcastMessage(msg domain.ChatMessage) {
	for id, client := range h.clients {
		if id != msg.ClientID {
			_, err := client.Conn.Write([]byte(domain.FormatMessage(msg)))
			if err != nil {
				h.Logger.Printf("ERROR error writing to client %s: %v\n", id, err)
				h.Stats.ErrorCount++
			}
		}
	}
}

func (h *Hub) HandleClient(ctx context.Context, conn net.Conn) {
	defer func() {
		if r := recover(); r != nil {
			h.Logger.Printf("ERROR recovered: %v", r)
		}
	}()

	client := h.setupClientConnection(conn)
	if client == nil {
		return
	}
	h.Logger.Printf("INFO Client %s connected\n", client.ID)
	defer h.cleanupClient(ctx, client)

	select {
	case <-ctx.Done():
		return
	case h.register <- client:
	}

	scanner := bufio.NewScanner(client.Conn)
	for scanner.Scan() {
		h.Logger.Printf("INFO client %s send message: %s\n", client.ID, scanner.Text())
		space := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(space, "/") {
			h.HandleCommand(ctx, client, space)
		} else {
			msg := domain.ParseIncomingMessage(scanner.Text(), client.ID)
			select {
			case <-ctx.Done():
				return
			case h.broadcast <- msg:
			}
			_, err := client.Conn.Write([]byte(domain.FormatMessage(msg) + "\n"))
			if err != nil {
				h.Stats.ErrorCount++
				h.Logger.Printf("ERROR error during send message client %s: %v\n", client, err)
			}
		}
		err := client.Conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		if err != nil {
			h.Stats.ErrorCount++
			h.Logger.Printf("ERROR Can't update deadline for user %s: %v\n", client.ID, err)
			return
		}
	}
	h.Logger.Printf("INFO Client %s disconnected\n", client.ID)
}

func (h *Hub) setupClientConnection(conn net.Conn) *domain.Client {
	err := conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		h.Stats.ErrorCount++
		h.Logger.Printf("ERROR error during set deadline: %v\n", err)
		return nil
	}
	client := &domain.Client{
		ID:       GenerateClientID(),
		Conn:     conn,
		JoinTime: time.Now(),
	}
	client.Conn.Write([]byte(domain.FormatMessage(domain.CreateSystemMessage(fmt.Sprintf("Welcome message to %s\n", client.ID)))))
	return client
}

func (h *Hub) cleanupClient(ctx context.Context, client *domain.Client) {
	err := client.Conn.Close()
	if err != nil {
		h.Logger.Printf("ERROR error during closing connection for user %s: %v\n", client.ID, err)
		return
	}
	select {
	case <-ctx.Done():
		return
	default:
		h.unregister <- client
		h.broadcast <- domain.CreateSystemMessage(fmt.Sprintf("Client %s disconnected (timeout)", client.ID))
	}
}

func (mh *MessageHistory) Add(msg domain.ChatMessage) {
	mh.buf[mh.head%cap(mh.buf)] = msg
	mh.head++
}

func (mh *MessageHistory) GetRecent() []domain.ChatMessage {
	size := len(mh.buf)

	count := mh.head
	if count > size {
		count = size
	}

	start := mh.head % size

	res := make([]domain.ChatMessage, 0, count)

	for i := 0; i < count; i++ {
		index := (start + i) % size
		res = append(res, mh.buf[index])
	}

	return res
}

func (h *Hub) SendUserList(client *domain.Client) {
	ids := make([]string, 0)
	for id, _ := range h.clients {
		ids = append(ids, id)
	}

	client.Conn.Write([]byte(domain.FormatMessage(domain.CreateSystemMessage(fmt.Sprintf("Online users (%d): %v\n", len(ids), ids)))))
}

func (h *Hub) HandleCommand(ctx context.Context, client *domain.Client, command string) {
	switch command {
	case "/users":
		h.SendUserList(client)
	case "/quit":
		h.cleanupClient(ctx, client)
	case "/help":
		client.Conn.Write([]byte(domain.FormatMessage(domain.CreateSystemMessage("Commands: /help, /users, /quit, /time"))))
	case "/time":
		recent := h.history.GetRecent()
		if len(recent) > 0 {
			client.Conn.Write([]byte(domain.FormatMessage(domain.CreateSystemMessage("--- Recent messages ---"))))

			for _, msg := range recent {
				client.Conn.Write([]byte(domain.FormatMessage(msg)))
			}

			client.Conn.Write([]byte(domain.FormatMessage(domain.CreateSystemMessage("--- End of history ---"))))
		}
	}
}

func (h *Hub) Shutdown(ctx context.Context) error {
	for _, client := range h.clients {
		_, err := client.Conn.Write(
			[]byte(domain.FormatMessage(
				domain.CreateSystemMessage("Server is shutting down"),
			)),
		)
		if err != nil {
			h.Logger.Printf(
				"ERROR failed to notify client %s: %v",
				client.ID,
				err,
			)
		}

		err = client.Conn.SetReadDeadline(time.Now())
		if err != nil {
			h.Logger.Printf(
				"ERROR failed to stop reading from client %s: %v",
				client.ID,
				err,
			)
		}
	}

	done := make(chan struct{})

	go func() {
		h.Wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		h.Logger.Println("INFO All client goroutines stopped")
		return nil

	case <-ctx.Done():
		return ctx.Err()
	}
}

func GenerateClientID() string {
	return fmt.Sprintf("User_%s", uuid.New())
}
