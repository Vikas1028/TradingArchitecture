package web

import (
	"encoding/json"
	"net/http"

	"market_price/internal/service"
)

func NewServer(store *service.Store) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/dashboard", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(store.Snapshot())
	})
	return mux
}
