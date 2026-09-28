package domain

import (
	"fmt"
	"net"
	"time"
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

type ServerStats struct {
	ActiveConnections      int64 `json:"active_connections"`
	TotalMessagesProcessed int64 `json:"total_messages_processed"`
	UptimeSeconds          int64 `json:"uptime_seconds"`
	ErrorCount             int64 `json:"error_count"`
	StartedAt              int64 `json:"-"`
}

func CreateSystemMessage(raw string) ChatMessage {
	return ChatMessage{
		Timestamp:   time.Time{},
		Content:     raw,
		MessageType: "system",
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
