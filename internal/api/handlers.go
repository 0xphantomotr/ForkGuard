package api

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/0xphantomotr/ForkGuard/internal/storage"
)

// --- Helper Functions ---

func generateSecret(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func (s *ApiServer) readJSON(w http.ResponseWriter, r *http.Request, dst interface{}) error {
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	if err := dec.Decode(dst); err != nil {
		return err
	}
	return nil
}

func (s *ApiServer) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if data != nil {
		json.NewEncoder(w).Encode(data)
	}
}

func (s *ApiServer) errorResponse(w http.ResponseWriter, status int, message string) {
	s.writeJSON(w, status, map[string]string{"error": message})
}

// --- Handlers ---

func (s *ApiServer) handleCreateSubscription(w http.ResponseWriter, r *http.Request) {
	var req CreateSubscriptionRequest
	if err := s.readJSON(w, r, &req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request payload")
		return
	}

	// For now, we'll use a hardcoded tenant. This would come from auth middleware in a real app.
	tenant := "test-tenant"

	secret, err := generateSecret(32)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "failed to generate secret")
		return
	}

	sub := req.toStorageSubscription(tenant, secret)

	if err := s.storage.CreateSubscription(r.Context(), sub); err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "failed to create subscription")
		return
	}

	s.writeJSON(w, http.StatusCreated, toCreateSubscriptionResponse(sub))
}

func (s *ApiServer) handleGetSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tenant := "test-tenant" // Hardcoded for now

	sub, err := s.storage.GetSubscription(r.Context(), id, tenant)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			s.errorResponse(w, http.StatusNotFound, "subscription not found")
		} else {
			s.errorResponse(w, http.StatusInternalServerError, "failed to retrieve subscription")
		}
		return
	}

	s.writeJSON(w, http.StatusOK, sub)
}

func (s *ApiServer) handleListSubscriptions(w http.ResponseWriter, r *http.Request) {
	tenant := "test-tenant" // Hardcoded for now

	subs, err := s.storage.ListSubscriptions(r.Context(), tenant)
	if err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "failed to list subscriptions")
		return
	}

	s.writeJSON(w, http.StatusOK, subs)
}

func (s *ApiServer) handleUpdateSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tenant := "test-tenant"

	// First, get the existing subscription
	existingSub, err := s.storage.GetSubscription(r.Context(), id, tenant)
	if err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			s.errorResponse(w, http.StatusNotFound, "subscription not found")
		} else {
			s.errorResponse(w, http.StatusInternalServerError, "failed to retrieve subscription")
		}
		return
	}

	// Decode the request into our update struct
	var req UpdateSubscriptionRequest
	if err := s.readJSON(w, r, &req); err != nil {
		s.errorResponse(w, http.StatusBadRequest, "invalid request payload")
		return
	}

	// Apply the updates from the request to the existing subscription
	if req.URL != nil {
		existingSub.URL = *req.URL
	}
	if req.Secret != nil {
		existingSub.Secret = *req.Secret
	}
	if req.MinConfs != nil {
		existingSub.MinConfs = *req.MinConfs
	}
	if req.Address != nil {
		existingSub.Address = req.Address
	}
	if req.Topic0 != nil {
		existingSub.Topic0 = req.Topic0
	}
	if req.Topic1 != nil {
		existingSub.Topic1 = req.Topic1
	}
	if req.Topic2 != nil {
		existingSub.Topic2 = req.Topic2
	}
	if req.Topic3 != nil {
		existingSub.Topic3 = req.Topic3
	}
	if req.FromBlock != nil {
		existingSub.FromBlock = req.FromBlock
	}
	if req.Active != nil {
		existingSub.Active = *req.Active
	}

	// Save the updated subscription back to the database
	if err := s.storage.UpdateSubscription(r.Context(), existingSub); err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "failed to update subscription")
		return
	}

	s.writeJSON(w, http.StatusOK, existingSub)
}

func (s *ApiServer) handleDeleteSubscription(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tenant := "test-tenant" // Hardcoded for now

	if err := s.storage.DeleteSubscription(r.Context(), id, tenant); err != nil {
		s.errorResponse(w, http.StatusInternalServerError, "failed to delete subscription")
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
