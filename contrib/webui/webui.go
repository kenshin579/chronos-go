// Package webui serves a browser-based task-management console for chronos-go,
// backed entirely by the public chronos.Inspector. Mount Handler in your own
// http.ServeMux, or run cmd/webui for a standalone server.
package webui

import (
	"io/fs"
	"net/http"

	"github.com/kenshin579/chronos-go"
)

// listStates are the ZSET-backed states a queue's tasks can be listed by.
var listStates = []string{"scheduled", "retry", "archived"}

// Handler returns the console's HTTP handler backed by insp.
func Handler(insp *chronos.Inspector) http.Handler {
	// 20 samples ≈ 10min at 30s or ~100s at 5s polling — the sampling cadence
	// follows whoever calls /api/stats or views the dashboard.
	s := &server{insp: insp, sparks: newSparkStore(20)}
	mux := http.NewServeMux()

	static, _ := fs.Sub(staticFS, "static")
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(static))))

	mux.HandleFunc("GET /{$}", s.dashboard)
	mux.HandleFunc("GET /api/stats", s.apiStats)
	mux.HandleFunc("GET /queues/{queue}", s.queueDetail)
	mux.HandleFunc("GET /queues/{queue}/tasks/{id}", s.taskDetail)
	mux.HandleFunc("POST /queues/{queue}/tasks/{id}/run", s.runTask)
	mux.HandleFunc("POST /queues/{queue}/tasks/{id}/delete", s.deleteTask)
	return mux
}

type server struct {
	insp   *chronos.Inspector
	sparks *sparkStore
}
