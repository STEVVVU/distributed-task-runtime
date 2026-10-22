package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"

	"distributed-task-runtime/internal/cluster"
	"distributed-task-runtime/internal/wire"
)

func main() {
	role := flag.String("role", "coordinator", "coordinator or worker")
	address := flag.String("address", "127.0.0.1:7100", "loopback gRPC address")
	coordinator := flag.String("coordinator", "127.0.0.1:7100", "coordinator address")
	id := flag.String("id", "worker1", "unique worker session ID")
	data := flag.String("data", "runtime.db", "bbolt file")
	python := flag.String("python", "python", "trusted Python interpreter")
	modules := flag.String("modules", "examples.functions", "trusted installed module allowlist")
	flag.Parse()
	token := os.Getenv("TASK_RUNTIME_TOKEN")
	if len(token) < 24 {
		log.Fatal("TASK_RUNTIME_TOKEN must contain at least 24 characters")
	}
	if !wire.Loopback(*address) || !wire.Loopback(*coordinator) {
		log.Fatal("only loopback addresses are supported")
	}
	if err := os.MkdirAll(filepath.Dir(*data), 0700); err != nil {
		log.Fatal(err)
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()
	switch *role {
	case "coordinator":
		c, err := cluster.OpenCoordinator(*data, *modules)
		if err != nil {
			log.Fatal(err)
		}
		defer c.Close()
		go c.Run(ctx)
		log.Printf("coordinator gRPC %s", *address)
		if err := wire.Serve(ctx, *address, token, c.Call); err != nil {
			log.Fatal(err)
		}
	case "worker":
		w := &cluster.Worker{ID: *id, Address: *address, Coordinator: *coordinator, Token: token, Python: *python, Modules: *modules}
		if err := w.Open(*data); err != nil {
			log.Fatal(err)
		}
		defer w.DB.Close()
		go w.Run(ctx)
		log.Printf("worker %s gRPC %s", *id, *address)
		if err := wire.Serve(ctx, *address, token, w.Call); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatal("unknown role")
	}
}
