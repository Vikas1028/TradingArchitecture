package main

import (
	"encoding/json"
	"net/http"

	"real_engine/internal/postgres"
)

func registerTradesAPI(store *postgres.TradeStore) {
	if store == nil || !store.Enabled() {
		return
	}
	http.HandleFunc("/trades", func(w http.ResponseWriter, r *http.Request) {
		since := r.URL.Query().Get("since")
		rows, err := store.LoadTradeHistory(r.Context(), since)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rows)
	})
}
