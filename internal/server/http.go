package server

import (
	"encoding/json"
	"net/http"
	"tcp-chat/internal/hub"
	"time"
)

func StartHTTPMonitoring(hub *hub.Hub, port string) {
	http.HandleFunc("/health", handleHealthEndpoint(hub))
	http.HandleFunc("/stats", handleStatsEndpoint(hub))
	http.ListenAndServe(port, nil)
}

func handleHealthEndpoint(hub *hub.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		hub.Stats.UptimeSeconds = time.Now().Unix() - hub.Stats.StartedAt

		response := struct {
			Status            string `json:"status"`
			ActiveConnections int    `json:"active_connections"`
			UptimeSeconds     int64  `json:"uptime_seconds"`
		}{
			Status:            "healthy",
			ActiveConnections: hub.Stats.ActiveConnections,
			UptimeSeconds:     hub.Stats.UptimeSeconds,
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
		hub.Stats.UptimeSeconds = time.Now().Unix() - hub.Stats.StartedAt
		stats, err := json.MarshalIndent(hub.Stats, "", " ")
		if err != nil {
			http.Error(w, "failed to marshal stats", http.StatusInternalServerError)
			return
		}
		w.Write(stats)
	}
}
