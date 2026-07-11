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

// render writes the named page (dashboard|queue|task) wrapped in the layout.
// Each page template defines "content"; we parse layout + the one page per call
// so the correct "content" block is bound.
func render(w http.ResponseWriter, page string, data any) {
	t := template.Must(template.ParseFS(templatesFS, "templates/layout.html", "templates/"+page+".html"))
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := t.ExecuteTemplate(w, "layout", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
