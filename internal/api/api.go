package api

import (
	"net/http"

	"github.com/0xphantomotr/ForkGuard/internal/storage"
)

// Holds the dependencies for the API server.
type ApiServer struct {
	listenAddr string
	storage    storage.Storage
}

// Creates a new ApiServer instance.
func NewApiServer(listenAddr string, storage storage.Storage) *ApiServer {
	return &ApiServer{
		listenAddr: listenAddr,
		storage:    storage,
	}
}

// Starts the HTTP server and registers the API routes.
func (s *ApiServer) Run() error {
	mux := http.NewServeMux()

	// Health check
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	// Subscription routes
	mux.HandleFunc("POST /v1/subscriptions", s.handleCreateSubscription)
	mux.HandleFunc("GET /v1/subscriptions", s.handleListSubscriptions)
	mux.HandleFunc("GET /v1/subscriptions/{id}", s.handleGetSubscription)
	mux.HandleFunc("PATCH /v1/subscriptions/{id}", s.handleUpdateSubscription)
	mux.HandleFunc("DELETE /v1/subscriptions/{id}", s.handleDeleteSubscription)

	server := &http.Server{
		Addr:    s.listenAddr,
		Handler: mux,
	}

	return server.ListenAndServe()
}
