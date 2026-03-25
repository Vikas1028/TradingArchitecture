package web

import (
	"encoding/json"
	"html/template"
	"net/http"
	"os/exec"
	"path/filepath"
	"strings"

	"trading_dashboard/internal/config"
	"trading_dashboard/internal/source"
)

type Server struct {
	pageTitle string
	poller    *source.Poller
	tmpl      *template.Template
	services  map[string]config.ServiceConfig
}

type pageData struct {
	PageTitle string
}

type servicePageData struct {
	PageTitle string
	ServiceID string
}

func NewServer(pageTitle string, poller *source.Poller, services []config.ServiceConfig) (*Server, error) {
	tmpl, err := template.ParseFS(templatesFS, "templates/*.html")
	if err != nil {
		return nil, err
	}

	serviceMap := make(map[string]config.ServiceConfig, len(services))
	for _, service := range services {
		serviceMap[service.ID] = service
	}

	return &Server{
		pageTitle: pageTitle,
		poller:    poller,
		tmpl:      tmpl,
		services:  serviceMap,
	}, nil
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleHome)
	mux.HandleFunc("/services", s.handleServices)
	mux.HandleFunc("/services/", s.handleServiceDetail)
	mux.HandleFunc("/api/services", s.handleServicesAPI)
	mux.HandleFunc("/api/services/", s.handleServiceDetailAPI)
	mux.HandleFunc("/api/actions/", s.handleServiceAction)
	return mux
}

func (s *Server) handleHome(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = s.tmpl.ExecuteTemplate(w, "home.html", pageData{PageTitle: s.pageTitle})
}

func (s *Server) handleServices(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/services" {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = s.tmpl.ExecuteTemplate(w, "services.html", pageData{PageTitle: s.pageTitle})
}

func (s *Server) handleServiceDetail(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/services/") {
		http.NotFound(w, r)
		return
	}
	serviceID := strings.TrimPrefix(r.URL.Path, "/services/")
	if serviceID == "" {
		http.NotFound(w, r)
		return
	}

	if _, ok := s.poller.SnapshotByID(serviceID); !ok {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_ = s.tmpl.ExecuteTemplate(w, "service_detail.html", servicePageData{
		PageTitle: s.pageTitle,
		ServiceID: serviceID,
	})
}

func (s *Server) handleServicesAPI(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/api/services" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.poller.SnapshotAll())
}

func (s *Server) handleServiceDetailAPI(w http.ResponseWriter, r *http.Request) {
	if !strings.HasPrefix(r.URL.Path, "/api/services/") {
		http.NotFound(w, r)
		return
	}
	serviceID := strings.TrimPrefix(r.URL.Path, "/api/services/")
	if serviceID == "" {
		http.NotFound(w, r)
		return
	}

	state, ok := s.poller.SnapshotByID(serviceID)
	if !ok {
		http.NotFound(w, r)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(state)
}

func (s *Server) handleServiceAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/actions/")
	parts := strings.Split(path, "/")
	if len(parts) != 2 {
		http.NotFound(w, r)
		return
	}

	service, ok := s.services[parts[0]]
	if !ok {
		http.NotFound(w, r)
		return
	}

	var script string
	switch parts[1] {
	case "start":
		script = service.StartScript
	case "stop":
		script = service.StopScript
	default:
		http.NotFound(w, r)
		return
	}

	if strings.TrimSpace(script) == "" {
		http.Error(w, "action not configured", http.StatusBadRequest)
		return
	}

	cleanPath := filepath.Clean(script)
	if !strings.HasPrefix(cleanPath, "/Users/vikasbhandekar/live_services/") {
		http.Error(w, "script path not allowed", http.StatusForbidden)
		return
	}

	output, err := exec.Command(cleanPath).CombinedOutput()
	resp := map[string]any{
		"ok":     err == nil,
		"output": strings.TrimSpace(string(output)),
	}
	if err != nil {
		resp["error"] = err.Error()
		w.WriteHeader(http.StatusInternalServerError)
	} else {
		s.poller.RefreshNow()
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}
