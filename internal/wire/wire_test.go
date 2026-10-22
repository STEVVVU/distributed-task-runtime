package wire

import (
	"context"
	"google.golang.org/protobuf/types/known/structpb"
	"math"
	"net"
	"testing"
	"time"
)

func TestServeStopsWithOwnedContext(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finished := make(chan error, 1)
	go func() {
		finished <- Serve(ctx, address, "synthetic-capability-token", func(context.Context, map[string]any) (any, error) { return map[string]any{"ok": true}, nil })
	}()
	deadline := time.Now().Add(3 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		var result map[string]any
		if Call(ctx, address, "synthetic-capability-token", map[string]any{}, &result) == nil {
			ready = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !ready {
		t.Fatal("service never ready")
	}
	cancel()
	select {
	case err := <-finished:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("service did not stop")
	}
}

func TestNonfiniteProtobufNumberRejectedBeforeJSONConversion(t *testing.T) {
	message, err := structpb.NewStruct(map[string]any{"value": math.NaN()})
	if err != nil {
		t.Fatal(err)
	}
	handler := Service(func(context.Context, map[string]any) (any, error) {
		t.Fatal("invalid number reached handler")
		return nil, nil
	})
	if _, err = handler.Call(context.Background(), message); err == nil {
		t.Fatal("nonfinite protobuf number accepted")
	}
}
