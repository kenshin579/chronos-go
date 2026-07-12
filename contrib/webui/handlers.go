package webui

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strings"

	"github.com/kenshin579/chronos-go"
)

// sparkW/sparkH are the sparkline SVG dimensions used everywhere.
const (
	sparkW = 80
	sparkH = 20
)

const listLimit = 100

// ctx aliases context.Context to keep the action func signature readable.
type ctx = context.Context

// pageData carries fields every page needs (embedded by page-specific data).
type pageData struct {
	Title string
	Conn  string
}

func (s *server) dashboard(w http.ResponseWriter, r *http.Request) {
	queues, err := s.insp.Queues(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sched, serr := s.insp.SchedulerStatus(r.Context())
	if serr != nil {
		sched = nil
	}
	// template.HTML is safe here: the SVG is entirely server-generated from
	// float samples (sparklineSVG), never from user input — no XSS surface.
	sparks := make(map[string]template.HTML, len(queues))
	for _, q := range queues {
		s.sparks.push(q.Queue, float64(q.Pending+q.Active))
		sparks[q.Queue] = template.HTML(s.sparks.svg(q.Queue, sparkW, sparkH))
	}
	render(w, "dashboard", struct {
		pageData
		Queues []*chronos.QueueInfo
		Sched  *chronos.SchedulerStatus
		Sparks map[string]template.HTML
		Msg    string
	}{
		pageData: pageData{Title: "queues", Conn: s.conn},
		Queues:   queues,
		Sched:    sched,
		Sparks:   sparks,
		Msg:      r.URL.Query().Get("msg"),
	})
}

// statsQueue is one queue's counters plus its server-rendered sparkline SVG.
type statsQueue struct {
	Queue     string `json:"queue"`
	Pending   int64  `json:"pending"`
	Active    int64  `json:"active"`
	Scheduled int64  `json:"scheduled"`
	Retry     int64  `json:"retry"`
	Archived  int64  `json:"archived"`
	Completed int64  `json:"completed"`
	Spark     string `json:"spark"`
}

// apiStats returns per-queue counters as JSON and feeds the sparkline rings —
// each poll is also a sample, so the pulse cadence follows the pollers.
func (s *server) apiStats(w http.ResponseWriter, r *http.Request) {
	queues, err := s.insp.Queues(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	out := make([]statsQueue, 0, len(queues))
	for _, q := range queues {
		s.sparks.push(q.Queue, float64(q.Pending+q.Active))
		out = append(out, statsQueue{
			Queue:     q.Queue,
			Pending:   q.Pending,
			Active:    q.Active,
			Scheduled: q.Scheduled,
			Retry:     q.Retry,
			Archived:  q.Archived,
			Completed: q.Completed,
			Spark:     s.sparks.svg(q.Queue, sparkW, sparkH),
		})
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	if err := json.NewEncoder(w).Encode(map[string]any{"queues": out}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
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
	if kf := r.URL.Query().Get("kind"); kf != "" {
		kept := tasks[:0]
		for _, t := range tasks {
			if t.Kind == kf {
				kept = append(kept, t)
			}
		}
		tasks = kept
	}
	render(w, "queue", struct {
		pageData
		Queue      string
		State      string
		States     []string
		Tasks      []*chronos.TaskInfo
		Msg        string
		KindFilter string
	}{
		pageData:   pageData{Title: queue, Conn: s.conn},
		Queue:      queue,
		State:      state,
		States:     listStates,
		Tasks:      tasks,
		Msg:        r.URL.Query().Get("msg"),
		KindFilter: r.URL.Query().Get("kind"),
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
	var members []string
	groupLookupOK := true
	if task.GroupID != "" && task.GroupQueue != "" {
		m, merr := s.insp.GroupMembers(r.Context(), task.GroupQueue, task.GroupID)
		if merr != nil {
			groupLookupOK = false // 실패를 "0명 남음(완료)"처럼 보이게 하지 않는다
		} else {
			members = m
		}
	}
	render(w, "task", struct {
		pageData
		Task          *chronos.TaskInfo
		Payload       string
		GroupMembers  []string
		GroupLookupOK bool
	}{
		pageData:      pageData{Title: id, Conn: s.conn},
		Task:          task,
		Payload:       formatPayload(task.Payload),
		GroupMembers:  members,
		GroupLookupOK: groupLookupOK,
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

// bulkLimit bounds how many tasks one bulk action processes (safety valve for
// enormous backlogs — repeat the action to continue).
const bulkLimit = 10000

func (s *server) bulkRun(w http.ResponseWriter, r *http.Request) {
	s.bulk(w, r, s.insp.RunTask, "re-ran")
}

func (s *server) bulkDelete(w http.ResponseWriter, r *http.Request) {
	s.bulk(w, r, s.insp.DeleteTask, "deleted")
}

// bulk drains every task in the queue's given state through fn (same Origin
// guard as single-task actions), then PRGs back with a summary.
func (s *server) bulk(w http.ResponseWriter, r *http.Request, fn func(ctx, string, string) error, verb string) {
	if o := r.Header.Get("Origin"); o != "" && o != "http://"+r.Host && o != "https://"+r.Host {
		http.Error(w, "cross-origin request rejected", http.StatusForbidden)
		return
	}
	queue := r.PathValue("queue")
	state := r.URL.Query().Get("state")
	if state == "" {
		state = "archived"
	}
	// Bulk stays limited to failure states: wiping thousands of scheduled or
	// retained-completed tasks in one unauthenticated POST is a footgun the UI
	// never offers — reject it server-side too.
	if state != "archived" && state != "retry" {
		http.Error(w, "bulk actions are limited to archived|retry", http.StatusBadRequest)
		return
	}
	okCount, failCount := 0, 0
	failed := map[string]bool{} // don't re-count (or re-attempt) tasks that already failed
	for okCount+failCount < bulkLimit {
		tasks, err := s.insp.ListTasks(r.Context(), queue, state, listLimit)
		if err != nil {
			http.Redirect(w, r, "/queues/"+queue+"?state="+url.QueryEscape(state)+"&msg="+url.QueryEscape("bulk failed: "+err.Error()), http.StatusSeeOther)
			return
		}
		progressed := false
		for _, tsk := range tasks {
			if okCount+failCount >= bulkLimit {
				break
			}
			if failed[tsk.ID] {
				continue
			}
			if err := fn(r.Context(), queue, tsk.ID); err != nil {
				failed[tsk.ID] = true
				failCount++
			} else {
				okCount++
				progressed = true
			}
		}
		if !progressed {
			break // nothing new succeeded this pass — remaining items are stuck
		}
	}
	msg := fmt.Sprintf("%d task(s) %s", okCount, verb)
	if failCount > 0 {
		msg += fmt.Sprintf(", %d failed", failCount)
	}
	http.Redirect(w, r, "/queues/"+queue+"?state="+url.QueryEscape(state)+"&msg="+url.QueryEscape(msg), http.StatusSeeOther)
}

// search finds a task by exact ID across every known queue and redirects to it.
func (s *server) search(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimSpace(r.URL.Query().Get("id"))
	if id == "" {
		http.Redirect(w, r, "/", http.StatusSeeOther)
		return
	}
	queues, err := s.insp.Queues(r.Context())
	if err == nil {
		for _, q := range queues {
			if _, gerr := s.insp.GetTask(r.Context(), q.Queue, id); gerr == nil {
				http.Redirect(w, r, "/queues/"+q.Queue+"/tasks/"+id, http.StatusSeeOther)
				return
			}
		}
	}
	http.Redirect(w, r, "/?msg="+url.QueryEscape("task not found: "+id), http.StatusSeeOther)
}

func (s *server) schedulerPage(w http.ResponseWriter, r *http.Request) {
	st, err := s.insp.SchedulerStatus(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	render(w, "scheduler", struct {
		pageData
		Sched *chronos.SchedulerStatus
	}{pageData{Title: "scheduler", Conn: s.conn}, st})
}
