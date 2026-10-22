//go:build benchmark

package main

import (
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestBenchmarkProfileRejectsUnsafeAddresses(t *testing.T) {
	for _, address := range []string{"0.0.0.0:9000", "localhost:9000", "127.0.0.1:0", "[::]:9000", "127.0.0.1:65536"} {
		t.Setenv("TASK_RUNTIME_PROFILE_ADDR", address)
		if stop, err := startBenchmarkProfile(); err == nil {
			stop()
			t.Fatalf("accepted %s", address)
		}
	}
}

func TestBenchmarkProfileListenerFailureIsSynchronous(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("TASK_RUNTIME_PROFILE_ADDR", listener.Addr().String())
	if stop, err := startBenchmarkProfile(); err == nil {
		stop()
		t.Fatal("accepted an occupied profile listener")
	}
}

func TestBenchmarkProfileExposesOnlyBoundedCPUProfile(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	t.Setenv("TASK_RUNTIME_PROFILE_ADDR", address)
	stop, err := startBenchmarkProfile()
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	client := &http.Client{Timeout: 5 * time.Second}
	for _, test := range []struct {
		path string
		code int
	}{{"/debug/pprof/cmdline", 404}, {"/debug/pprof/profile?seconds=61", 400}, {"/debug/pprof/profile?seconds=1", 200}} {
		response, err := client.Get("http://" + address + test.path)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(response.Body)
		response.Body.Close()
		if err != nil || response.StatusCode != test.code {
			t.Fatalf("%s: status %d, error %v", test.path, response.StatusCode, err)
		}
		if test.code == 200 && (len(data) < 2 || data[0] != 0x1f || data[1] != 0x8b) {
			t.Fatal("missing gzip CPU profile")
		}
	}
}
