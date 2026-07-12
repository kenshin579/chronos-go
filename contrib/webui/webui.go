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
var listStates = []string{"scheduled", "retry", "archived", "completed"}

// HandlerOption customizes the console handler.
type HandlerOption func(*server)

// WithConnInfo sets the connection label shown in the header (e.g.
// "standalone 127.0.0.1:6379 db0" or "cluster (3 seed)").
func WithConnInfo(label string) HandlerOption {
	return func(s *server) { s.conn = label }
}

// Handler returns the console's HTTP handler backed by insp.
func Handler(insp *chronos.Inspector, opts ...HandlerOption) http.Handler {
	// 20 samples ≈ 10min at 30s or ~100s at 5s polling — the sampling cadence
	// follows whoever calls /api/stats or views the dashboard.
	s := &server{insp: insp, sparks: newSparkStore(20)}
	for _, opt := range opts {
		opt(s)
	}
	mux := http.NewServeMux()

	static, _ := fs.Sub(staticFS, "static")
	mux.Handle("GET /static/", http.StripPrefix("/static/", http.FileServer(http.FS(static))))

	mux.HandleFunc("GET /{$}", s.dashboard)
	mux.HandleFunc("GET /api/stats", s.apiStats)
	mux.HandleFunc("GET /queues/{queue}", s.queueDetail)
	mux.HandleFunc("GET /queues/{queue}/tasks/{id}", s.taskDetail)
	mux.HandleFunc("POST /queues/{queue}/tasks/{id}/run", s.runTask)
	mux.HandleFunc("POST /queues/{queue}/tasks/{id}/delete", s.deleteTask)
	mux.HandleFunc("POST /queues/{queue}/pause", s.pauseQueue)
	mux.HandleFunc("POST /queues/{queue}/resume", s.resumeQueue)
	mux.HandleFunc("POST /queues/{queue}/bulk/run", s.bulkRun)
	mux.HandleFunc("POST /queues/{queue}/bulk/delete", s.bulkDelete)
	mux.HandleFunc("GET /search", s.search)
	mux.HandleFunc("GET /scheduler", s.schedulerPage)
	return mux
}

type server struct {
	conn   string // connection label for the header ("" = hidden)
	insp   *chronos.Inspector
	sparks *sparkStore
}
