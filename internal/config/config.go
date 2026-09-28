package config

import (
	"flag"
	"fmt"
)

type ServerConfig struct {
	Port               string
	MaxConnections     int
	LogLevel           string
	MessageHistorySize int
}

func ParseCommandLineArgs() ServerConfig {
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

func PrintStartupBanner(config ServerConfig) {
	fmt.Println(`╔══════════════════════════════════════╗
║         TCP Chat Server              ║
╚══════════════════════════════════════╝`)

	fmt.Printf("Port:            %s\n", config.Port)
	fmt.Printf("Max Connections: %d\n", config.MaxConnections)
	fmt.Printf("Log Level:       %s\n", config.LogLevel)
	fmt.Printf("Connect using:   telnet localhost %s\n", config.Port)
}
