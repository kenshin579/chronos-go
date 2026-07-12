package webui

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/url"

	"github.com/kenshin579/chronos-go"
)

const listLimit = 100

// ctx aliases context.Context to keep the action func signature readable.
type ctx = context.Context

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
		if errors.Is(err, chronos.ErrInvalidState) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
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
func (s *server) taskDetail(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("queue")
	id := r.PathValue("id")
	task, err := s.insp.GetTask(r.Context(), queue, id)
	if err != nil {
		if errors.Is(err, chronos.ErrTaskNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
		} else {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	render(w, "task", struct {
		Title   string
		Task    *chronos.TaskInfo
		Payload string
	}{
		Title:   id,
		Task:    task,
		Payload: formatPayload(task.Payload),
	})
}

// formatPayload renders a payload for display: pretty-printed JSON when it
// parses, otherwise the raw string.
func formatPayload(p []byte) string {
	var buf bytes.Buffer
	if json.Valid(p) {
		if err := json.Indent(&buf, p, "", "  "); err == nil {
			return buf.String()
		}
	}
	return string(p)
}

func (s *server) runTask(w http.ResponseWriter, r *http.Request) {
	s.action(w, r, s.insp.RunTask, "queued for immediate run")
}

func (s *server) deleteTask(w http.ResponseWriter, r *http.Request) {
	s.action(w, r, s.insp.DeleteTask, "deleted")
}

// action runs a mutating Inspector call then redirects (PRG) back to the queue.
func (s *server) action(w http.ResponseWriter, r *http.Request, fn func(ctx, string, string) error, okMsg string) {
	if o := r.Header.Get("Origin"); o != "" && o != "http://"+r.Host && o != "https://"+r.Host {
		http.Error(w, "cross-origin request rejected", http.StatusForbidden)
		return
	}
	queue := r.PathValue("queue")
	id := r.PathValue("id")
	msg := okMsg
	if err := fn(r.Context(), queue, id); err != nil {
		msg = "error: " + err.Error()
	}
	http.Redirect(w, r, "/queues/"+queue+"?msg="+url.QueryEscape(msg), http.StatusSeeOther)
}
