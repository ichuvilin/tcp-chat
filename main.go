package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
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
}

type Request struct {
	ActiveUserResponse chan []string
	CountUserResponse  chan int
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

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client.ID] = client
		case client := <-h.unregister:
			if _, ok := h.clients[client.ID]; ok {
				delete(h.clients, client.ID)
			}
		case message := <-h.broadcast:
			h.BroadcastMessage(message)
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
				fmt.Printf("error writing to client %s: %v\n", id, err)
			}
		}
	}
}

func StartEchoServer(port string, h *Hub) error {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}
	fmt.Printf("TCP Chat Server listening on %s\n", port)
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		go handleClient(conn, GenerateClientID(), h)
	}
	return nil
}

func FormatMessage(msg ChatMessage) string {
	if msg.MessageType == "user" {
		return fmt.Sprintf("[%s] <%s>: %s", msg.Timestamp.Format("15:04:05"), msg.ClientID, msg.Content)
	}

	return fmt.Sprintf("[%s] *** %s", msg.Timestamp.Format("15:04:05"), msg.Content)
}

func ParseIncomingMessage(raw string, senderID string) ChatMessage {
	return ChatMessage{
		Timestamp:   time.Now(),
		Content:     raw,
		ClientID:    senderID,
		MessageType: "user",
	}
}

func HandleClient(client *Client) error {
	scanner := bufio.NewScanner(client.Conn)
	for scanner.Scan() {
		msg := FormatMessage(ParseIncomingMessage(scanner.Text(), client.ID))
		_, err := client.Conn.Write([]byte(msg + "\n"))
		if err != nil {
			return err
		}
	}
	return nil
}

func handleClient(conn net.Conn, clientID string, h *Hub) {
	fmt.Printf("user %s connect\n", clientID)
	client := h.setupClientConnection(conn)
	defer h.cleanupClient(client)

	h.register <- client
	scanner := bufio.NewScanner(client.Conn)
	for scanner.Scan() {
		fmt.Printf("user %s send message: %s\n", client, scanner.Text())
		msg := ParseIncomingMessage(scanner.Text(), client.ID)
		h.broadcast <- msg
		_, err := client.Conn.Write([]byte(FormatMessage(msg) + "\n"))
		if err != nil {
			fmt.Printf("error during send message client %s: %v\n", client, err)
		}
		client = h.setupClientConnection(conn)
	}
	fmt.Printf("user %s disconnected\n", client)
}

func GenerateClientID() string {
	return fmt.Sprintf("User_%s", uuid.New())
}

func (h *Hub) setupClientConnection(conn net.Conn) *Client {
	err := conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	if err != nil {
		fmt.Printf("error during set deadline: %s\n", err)
		return nil
	}
	client := &Client{
		ID:       GenerateClientID(),
		Conn:     conn,
		JoinTime: time.Now(),
	}
	client.Conn.Write([]byte(FormatMessage(ChatMessage{
		Timestamp:   time.Now(),
		Content:     fmt.Sprintf("Welcome message to %s", client.ID),
		MessageType: "system",
	})))
	return client
}

func (h *Hub) cleanupClient(client *Client) {
	err := client.Conn.Close()
	if err != nil {
		fmt.Printf("error during closing conn for user %s\n", client.ID)
		return
	}
	h.unregister <- client
	h.broadcast <- ChatMessage{
		Timestamp:   time.Now(),
		Content:     fmt.Sprintf("Client %s disconnected (timeout)", client.ID),
		MessageType: "system",
	}
}

func main() {
	h := &Hub{
		clients:    make(map[string]*Client),
		broadcast:  make(chan ChatMessage),
		register:   make(chan *Client),
		unregister: make(chan *Client),
		req:        make(chan Request),
	}
	go h.Run()
	err := StartEchoServer(":8080", h)
	if err != nil {
		fmt.Printf("error during start server: %v\n", err)
		os.Exit(1)
	}
}
