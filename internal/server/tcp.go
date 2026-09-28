package server

import (
	"context"
	"fmt"
	"net"
	"tcp-chat-server/internal/config"
	"tcp-chat-server/internal/hub"
)

func StartEchoServer(ctx context.Context, cfg config.ServerConfig, h *hub.Hub) error {
	listener, err := net.Listen("tcp", fmt.Sprintf(":%s", cfg.Port))
	if err != nil {
		return err
	}
	defer listener.Close()
	h.Logger.Printf("INFO TCP Chat Server listening on %s\n", cfg.Port)

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		count, err := h.GetClientCount(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}

			return err
		}

		if count >= cfg.MaxConnections {
			conn.Close()
			continue
		}

		h.Wg.Add(1)
		go func() {
			defer h.Wg.Done()

			h.HandleClient(ctx, conn)
		}()
	}
}
