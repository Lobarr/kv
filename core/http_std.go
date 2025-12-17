package core

import (
	"context"
	"encoding/json"
	"net/http"
)

type HttpStdServer struct {
	engine *Engine
	mux    *http.ServeMux
	server *http.Server
}

func NewHttpStdServer(engine *Engine) *HttpStdServer {
	mux := http.NewServeMux()
	server := &HttpStdServer{
		engine: engine,
		mux:    mux,
	}
	server.setupRoutes()
	return server
}

func (s *HttpStdServer) setupRoutes() {
	s.mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})
	s.mux.HandleFunc("/batch/set", s.batchSet)
	s.mux.HandleFunc("/batch/get", s.batchGet)
	s.mux.HandleFunc("/batch/delete", s.batchDelete)
}

func (s *HttpStdServer) batchSet(w http.ResponseWriter, r *http.Request) {
	var req BatchSetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for _, item := range req.Items {
		if err := s.engine.Set(item.Key, item.Value); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *HttpStdServer) batchGet(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Keys []string `json:"keys"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	res := BatchGetResponse{}
	for _, key := range req.Keys {
		value, err := s.engine.Get(key)
		if err != nil {
			if err == ErrKeyNotFound {
				continue
			}
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		res.Items = append(res.Items, struct {
			Key   string `json:"key"`
			Value string `json:"value"`
		}{Key: key, Value: value})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(res)
}

func (s *HttpStdServer) batchDelete(w http.ResponseWriter, r *http.Request) {
	var req BatchDeleteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	for _, key := range req.Keys {
		if err := s.engine.Delete(key); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

func (s *HttpStdServer) Start(addr string) error {
	s.server = &http.Server{
		Addr:    addr,
		Handler: s.mux,
	}
	if err := s.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func (s *HttpStdServer) Shutdown(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}
