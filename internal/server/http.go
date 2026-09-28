package server

import (
	"encoding/json"
	"net/http"
	"tcp-chat-server/internal/hub"
)

func StartHTTPMonitoring(hub *hub.Hub, port string) {
	http.HandleFunc("/health", handleHealthEndpoint(hub))
	http.HandleFunc("/stats", handleStatsEndpoint(hub))
	err := http.ListenAndServe(port, nil)
	if err != nil {
		hub.Logger.Printf("ERROR Can't start http client: %v", err)
		return
	}
}

func handleHealthEndpoint(hub *hub.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		stats := hub.GetStats()

		response := struct {
			Status            string `json:"status"`
			ActiveConnections int64  `json:"active_connections"`
			UptimeSeconds     int64  `json:"uptime_seconds"`
		}{
			Status:            "healthy",
			ActiveConnections: stats.ActiveConnections,
			UptimeSeconds:     stats.UptimeSeconds,
		}

		if err := json.NewEncoder(w).Encode(response); err != nil {
			http.Error(w, "failed to encode response", http.StatusInternalServerError)
			return
		}
	}
}

func handleStatsEndpoint(hub *hub.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		stats := hub.GetStats()

		data, err := json.MarshalIndent(stats, "", " ")
		if err != nil {
			http.Error(w, "failed to marshal stats", http.StatusInternalServerError)
			return
		}
		w.Write(data)
	}
}
