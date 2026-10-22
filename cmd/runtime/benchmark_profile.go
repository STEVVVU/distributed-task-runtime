//go:build benchmark

package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"strconv"
	"time"

	"distributed-task-runtime/internal/wire"
)

func startBenchmarkProfile() (func(), error) {
	address := os.Getenv("TASK_RUNTIME_PROFILE_ADDR")
	if address == "" {
		return func() {}, nil
	}
	if !wire.Loopback(address) {
		return nil, fmt.Errorf("benchmark profile requires a numeric loopback address and port")
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("benchmark profile listen: %w", err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/pprof/profile", func(w http.ResponseWriter, r *http.Request) {
		seconds, err := strconv.Atoi(r.URL.Query().Get("seconds"))
		if err != nil || seconds < 1 || seconds > 60 {
			http.Error(w, "profile seconds must be 1..60", http.StatusBadRequest)
			return
		}
		pprof.Profile(w, r)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: time.Second, ReadTimeout: 3 * time.Second, WriteTimeout: 65 * time.Second, IdleTimeout: time.Second}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("benchmark profile server: %v", err)
		}
	}()
	return func() { _ = server.Close() }, nil
}
