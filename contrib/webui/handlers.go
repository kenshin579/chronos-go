package webui

import (
	"net/http"

	"github.com/kenshin579/chronos-go"
)

func (s *server) dashboard(w http.ResponseWriter, r *http.Request) {
	queues, err := s.insp.Queues(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	render(w, "dashboard", struct {
		Title  string
		Queues []*chronos.QueueInfo
	}{Title: "queues", Queues: queues})
}

func (s *server) queueDetail(w http.ResponseWriter, r *http.Request) { http.Error(w, "todo", 501) }
func (s *server) taskDetail(w http.ResponseWriter, r *http.Request)  { http.Error(w, "todo", 501) }
func (s *server) runTask(w http.ResponseWriter, r *http.Request)     { http.Error(w, "todo", 501) }
func (s *server) deleteTask(w http.ResponseWriter, r *http.Request)  { http.Error(w, "todo", 501) }
