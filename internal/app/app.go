package app

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"tcp-chat/internal/config"
	"tcp-chat/internal/hub"
	"tcp-chat/internal/server"
	"time"
)

func New() {
	cfg := config.ParseCommandLineArgs()

	logger := setupLogging(cfg.LogLevel)

	h := hub.NewHub(logger, cfg.MessageHistorySize)

	config.PrintStartupBanner(cfg)

	signals := setupSignalHandling()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go h.Run(ctx)

	go server.StartHTTPMonitoring(h, "9090")

	go func() {
		err := server.StartEchoServer(ctx, cfg, h)
		if err != nil {
			logger.Printf("ERROR error during start server: %v\n", err)
			os.Exit(1)
		}
	}()

	sig := <-signals

	logger.Printf("INFO Received signal: %v", sig)

	cancel()

	_, shutdownCancel := context.WithTimeout(
		context.Background(),
		10*time.Second,
	)
	defer shutdownCancel()

	if err := h.Shutdown(); err != nil {
		logger.Printf("ERROR shutdown failed: %v", err)
	}
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

func setupLogging(level string) *log.Logger {
	return log.New(
		os.Stdout,
		"[TCP-CHAT] ",
		log.Ldate|log.Ltime,
	)
}
