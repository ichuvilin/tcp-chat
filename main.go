package main

import (
	"bufio"
	"fmt"
	"net"
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

func StartEchoServer(port string) error {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}
	defer listener.Close()
	for {
		conn, err := listener.Accept()
		if err != nil {
			return err
		}
		defer conn.Close()
		client := &Client{ID: GenerateClientID(), Conn: conn, JoinTime: time.Now()}
		err = HandleClient(client)
		if err != nil {
			fmt.Printf("error during handle client: %v\n", err)
		}
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

func GenerateClientID() string {
	return fmt.Sprintf("User_%s", uuid.New())
}

func main() {
	StartEchoServer(":8080")
}
