package webui

import (
	"net/http"

	"github.com/kenshin579/chronos-go"
)

const listLimit = 100

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

func (s *server) queueDetail(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("queue")
	state := r.URL.Query().Get("state")
	if state == "" {
		state = "archived"
	}
	tasks, err := s.insp.ListTasks(r.Context(), queue, state, listLimit)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	render(w, "queue", struct {
		Title  string
		Queue  string
		State  string
		States []string
		Tasks  []*chronos.TaskInfo
		Msg    string
	}{
		Title:  queue,
		Queue:  queue,
		State:  state,
		States: listStates,
		Tasks:  tasks,
		Msg:    r.URL.Query().Get("msg"),
	})
}
func (s *server) taskDetail(w http.ResponseWriter, r *http.Request) { http.Error(w, "todo", 501) }
func (s *server) runTask(w http.ResponseWriter, r *http.Request)    { http.Error(w, "todo", 501) }
func (s *server) deleteTask(w http.ResponseWriter, r *http.Request) { http.Error(w, "todo", 501) }
