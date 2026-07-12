package webui

import (
	"embed"
	"html/template"
	"net/http"
)

//go:embed templates/*.html
var templatesFS embed.FS

//go:embed static/*
var staticFS embed.FS

// tmplFuncs are small helpers available to every page template.
var tmplFuncs = template.FuncMap{
	"add": func(a, b int) int { return a + b },
	"seq": func(n int) []int {
		out := make([]int, n)
		for i := range out {
			out[i] = i
		}
		return out
	},
	"trunc": func(s string, n int) string {
		if len(s) <= n {
			return s
		}
		return s[:n] + "…"
	},
}

// pages holds each content page parsed together with the shared layout, once.
var pages = map[string]*template.Template{
	"dashboard": mustPage("dashboard"),
	"queue":     mustPage("queue"),
	"task":      mustPage("task"),
	"scheduler": mustPage("scheduler"),
}

func mustPage(name string) *template.Template {
	return template.Must(template.New("layout.html").Funcs(tmplFuncs).
		ParseFS(templatesFS, "templates/layout.html", "templates/"+name+".html"))
}

// render writes the named page wrapped in the layout.
func render(w http.ResponseWriter, page string, data any) {
	t, ok := pages[page]
	if !ok {
		http.Error(w, "unknown page: "+page, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.ExecuteTemplate(w, "layout", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
