package webui

import (
	"bytes"
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
		r := []rune(s) // rune-wise: byte slicing would cut multi-byte text mid-rune
		if len(r) <= n {
			return s
		}
		return string(r[:n]) + "…"
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
	// Render to a buffer first: a template error mid-stream would otherwise
	// leave partial HTML followed by a broken http.Error.
	var buf bytes.Buffer
	if err := t.ExecuteTemplate(&buf, "layout", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = buf.WriteTo(w)
}
