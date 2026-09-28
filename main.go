package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	"uuid"
)

type ChatMessage struct {
	Timestamp   time.Time
	ClientID    string
	Content     string
	MessageType string
}

type Client struct {
	ID       string
	Conn     net.Conn
	JoinTime time.Time
}

type Hub struct {
	clients    map[string]*Client
	broadcast  chan ChatMessage
	register   chan *Client
	unregister chan *Client
	req        chan Request
	history    MessageHistory
	logger     *log.Logger
	stats      *ServerStats
	wg         sync.WaitGroup
}

type ServerStats struct {
	ActiveConnections      int   `json:"active_connections"`
	TotalMessagesProcessed int64 `json:"total_messages_processed"`
	UptimeSeconds          int64 `json:"uptime_seconds"`
	ErrorCount             int   `json:"error_count"`
	StartedAt              int64 `json:"-"`
}

type MessageHistory struct {
	buf  []ChatMessage
	head int
}

type Request struct {
	ActiveUserResponse chan []string
	CountUserResponse  chan int
}

type ServerConfig struct {
	Port               string
	MaxConnections     int
	LogLevel           string
	MessageHistorySize int
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
			h.stats.ActiveConnections++
		case client := <-h.unregister:
			if _, ok := h.clients[client.ID]; ok {
				delete(h.clients, client.ID)
				h.stats.ActiveConnections--
			}
		case message, ok := <-h.broadcast:
			if !ok {
				return
			}
			h.BroadcastMessage(message)
			h.history.Add(message)
			h.stats.TotalMessagesProcessed++
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

func (h *Hub) BroadcastMessage(msg ChatMessage) {
	for id, client := range h.clients {
		if id != msg.ClientID {
			_, err := client.Conn.Write([]byte(FormatMessage(msg)))
			if err != nil {
				h.logger.Printf("ERROR error writing to client %s: %v\n", id, err)
				h.stats.ErrorCount++
			}
		}
	}
}

func StartEchoServer(ctx context.Context, cfg ServerConfig, h *Hub) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.Port))
	if err != nil {
		return err
	}
	h.logger.Printf("INFO TCP Chat Server listening on %s\n", cfg.Port)
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		count := h.GetClientCount()
		if count >= cfg.MaxConnections {
			conn.Close()
			continue
		}
		h.wg.Add(1)
		go func() {
			defer h.wg.Done()

			handleClient(ctx, conn, GenerateClientID(), h)
		}()
	}
}

func FormatMessage(msg ChatMessage) string {
	if msg.MessageType == "user" {
		return fmt.Sprintf("[%s] <%s>: %s\n", msg.Timestamp.Format("15:04:05"), msg.ClientID, msg.Content)
	}

	return fmt.Sprintf("[%s] *** %s\n", msg.Timestamp.Format("15:04:05"), msg.Content)
}

func ParseIncomingMessage(raw string, senderID string) ChatMessage {
	return ChatMessage{
		Timestamp:   time.Now(),
		Content:     raw,
		ClientID:    senderID,
		MessageType: "user",
	}
}

func handleClient(ctx context.Context, conn net.Conn, clientID string, h *Hub) {
	defer func() {
		if r := recover(); r != nil {
			h.logger.Printf("ERROR recovered: %v", r)
		}
	}()

	h.logger.Printf("INFO Client %s connected\n", clientID)
	client := h.setupClientConnection(conn)
	if client == nil {
		return
	}
	defer h.cleanupClient(client)

	select {
	case <-ctx.Done():
		return
	case h.register <- client:
	}

	scanner := bufio.NewScanner(client.Conn)
	for scanner.Scan() {
		h.logger.Printf("INFO client %s send message: %s\n", client.ID, scanner.Text())
		space := strings.TrimSpace(scanner.Text())
		if strings.HasPrefix(space, "/") {
			h.HandleCommand(client, space)
		} else {
			msg := ParseIncomingMessage(scanner.Text(), client.ID)
			h.broadcast <- msg
			_, err := client.Conn.Write([]byte(FormatMessage(msg) + "\n"))
			if err != nil {
				h.stats.ErrorCount++
				h.logger.Printf("ERROR error during send message client %s: %v\n", client, err)
			}
		}
		err := client.Conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		if err != nil {
			h.stats.ErrorCount++
			h.logger.Printf("ERROR Can't update deadline for user %s: %v\n", client.ID, err)
			return
		}
	}
	h.logger.Printf("INFO Client %s disconnected\n", client.ID)
}

func GenerateClientID() string {
	return fmt.Sprintf("User_%s", uuid.New())
}

func (h *Hub) setupClientConnection(conn net.Conn) *Client {
	err := conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		h.stats.ErrorCount++
		h.logger.Printf("ERROR error during set deadline: %v\n", err)
		return nil
	}
	client := &Client{
		ID:       GenerateClientID(),
		Conn:     conn,
		JoinTime: time.Now(),
	}
	client.Conn.Write([]byte(FormatMessage(ChatMessage{
		Timestamp:   time.Now(),
		Content:     fmt.Sprintf("Welcome message to %s\n", client.ID),
		MessageType: "system",
	})))
	return client
}

func (h *Hub) cleanupClient(client *Client) {
	err := client.Conn.Close()
	if err != nil {
		h.logger.Printf("ERROR error during closing connection for user %s: %v\n", client.ID, err)
		return
	}
	h.unregister <- client
	h.broadcast <- ChatMessage{
		Timestamp:   time.Now(),
		Content:     fmt.Sprintf("Client %s disconnected (timeout)", client.ID),
		MessageType: "system",
	}
}

func (mh *MessageHistory) Add(msg ChatMessage) {
	mh.buf[mh.head%50] = msg
	mh.head++
}

func (mh *MessageHistory) GetRecent() []ChatMessage {
	size := len(mh.buf)

	count := mh.head
	if count > size {
		count = size
	}

	start := mh.head % size

	res := make([]ChatMessage, 0, count)

	for i := 0; i < count; i++ {
		index := (start + i) % size
		res = append(res, mh.buf[index])
	}

	return res
}

func (h *Hub) SendUserList(client *Client) {
	ids := make([]string, 0)
	for id, _ := range h.clients {
		ids = append(ids, id)
	}

	client.Conn.Write([]byte(FormatMessage(ChatMessage{
		Timestamp:   time.Now(),
		Content:     fmt.Sprintf("Online users (%d): %v\n", len(ids), ids),
		MessageType: "system",
	})))
}

func (h *Hub) HandleCommand(client *Client, command string) {
	switch command {
	case "/users":
		h.SendUserList(client)
	case "/quit":
		h.cleanupClient(client)
	case "/help":
		client.Conn.Write([]byte(FormatMessage(ChatMessage{
			Timestamp:   time.Now(),
			Content:     "Commands: /help, /users, /quit, /time",
			MessageType: "system",
		})))
	case "/time":
		recent := h.history.GetRecent()
		if len(recent) > 0 {
			client.Conn.Write([]byte(FormatMessage(ChatMessage{
				Timestamp:   time.Now(),
				Content:     "--- Recent messages ---",
				MessageType: "system",
			})))

			for _, msg := range recent {
				client.Conn.Write([]byte(FormatMessage(msg)))
			}

			client.Conn.Write([]byte(FormatMessage(ChatMessage{
				Timestamp:   time.Now(),
				Content:     "--- End of history ---",
				MessageType: "system",
			})))
		}
	}
}

func setupLogging(level string) *log.Logger {
	return log.New(
		os.Stdout,
		"[TCP-CHAT] ",
		log.Ldate|log.Ltime,
	)
}

func setupSignalHandling() chan os.Signal {
	signals := make(chan os.Signal, 1)

	signal.Notify(
		signals,
		os.Interrupt,
		syscall.SIGTERM,
	)

	return signals
}

func (h *Hub) Shutdown(ctx context.Context) error {
	for _, client := range h.clients {
		_, err := client.Conn.Write(
			[]byte(FormatMessage(ParseIncomingMessage("Server is shutting down", ""))),
		)

		if err != nil {
			h.logger.Printf(
				"ERROR failed to notify client %s: %v",
				client.ID,
				err,
			)
		}
	}

	done := make(chan struct{})

	go func() {
		h.wg.Wait()
		close(done)
	}()

	close(h.broadcast)

	return nil
}

func parseCommandLineArgs() ServerConfig {
	port := flag.String("port", "8080", "порт сервера")
	maxConn := flag.Int("max-conn", 50, "лимит соединений")
	logLevel := flag.String("log-level", "info", "уровень логирования")
	historySize := flag.Int("history-size", 50, "размер истории")

	flag.Parse()

	return ServerConfig{
		Port:               *port,
		MaxConnections:     *maxConn,
		LogLevel:           *logLevel,
		MessageHistorySize: *historySize,
	}
}

func printStartupBanner(config ServerConfig) {
	fmt.Println(`╔══════════════════════════════════════╗
║         TCP Chat Server              ║
╚══════════════════════════════════════╝`)

	fmt.Printf("Port:            %s\n", config.Port)
	fmt.Printf("Max Connections: %d\n", config.MaxConnections)
	fmt.Printf("Log Level:       %s\n", config.LogLevel)
	fmt.Printf("Connect using:   telnet localhost %s\n", config.Port)
}

func startHTTPMonitoring(hub *Hub, port string) {
	http.HandleFunc("/health", handleHealthEndpoint(hub))
	http.HandleFunc("/stats", handleStatsEndpoint(hub))
	http.ListenAndServe(port, nil)
}

func handleHealthEndpoint(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		hub.stats.UptimeSeconds = time.Now().Unix() - hub.stats.StartedAt

		response := struct {
			Status            string `json:"status"`
			ActiveConnections int    `json:"active_connections"`
			UptimeSeconds     int64  `json:"uptime_seconds"`
		}{
			Status:            "healthy",
			ActiveConnections: hub.stats.ActiveConnections,
			UptimeSeconds:     hub.stats.UptimeSeconds,
		}

		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, "failed to encode response", http.StatusInternalServerError)
			return
		}
	}
}

func handleStatsEndpoint(hub *Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		hub.stats.UptimeSeconds = time.Now().Unix() - hub.stats.StartedAt
		stats, err := json.MarshalIndent(hub.stats, "", " ")
		if err != nil {
			http.Error(w, "failed to marshal stats", http.StatusInternalServerError)
			return
		}
		w.Write(stats)
	}
}

func main() {
	cfg := parseCommandLineArgs()

	logger := setupLogging(cfg.LogLevel)
	h := &Hub{
		clients:    make(map[string]*Client),
		broadcast:  make(chan ChatMessage),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		req:        make(chan Request),
		history:    MessageHistory{buf: make([]ChatMessage, cfg.MessageHistorySize), head: 0},
		logger:     logger,
		stats:      &ServerStats{StartedAt: time.Now().Unix()},
	}

	printStartupBanner(cfg)

	signals := setupSignalHandling()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go h.Run(ctx)

	go startHTTPMonitoring(h, ":9090")

	go func() {
		err := StartEchoServer(ctx, cfg, h)
		if err != nil {
			logger.Printf("ERROR error during start server: %v\n", err)
			os.Exit(1)
		}
	}()

	sig := <-signals

	logger.Printf("INFO Received signal: %v", sig)

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer shutdownCancel()

	if err := h.Shutdown(shutdownCtx); err != nil {
		logger.Printf("ERROR shutdown failed: %v", err)
	}
}
