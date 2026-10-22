package wire

import (
	"context"
	"encoding/json"
	"google.golang.org/protobuf/types/known/structpb"
	"math"
	"net"
	"strings"
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

func TestObjectMustFitProtobufFetchResponse(t *testing.T) {
	value := json.RawMessage("[" + strings.Repeat("0,", 199999) + "0]")
	if err := ValidateObject(value); err == nil || !strings.Contains(err.Error(), "protobuf RPC budget") {
		t.Fatalf("unfetchable object accepted: %v", err)
	}
}

func TestLoopbackAddressHasBoundedNumericPort(t *testing.T) {
	for _, address := range []string{"127.0.0.1:0", "127.0.0.1:-1", "127.0.0.1:65536", "127.0.0.1:abc", "127.0.0.1:000001", "192.0.2.1:80"} {
		if Loopback(address) {
			t.Fatalf("invalid peer accepted: %s", address)
		}
	}
	if !Loopback("127.0.0.1:7100") || !Loopback("[::1]:7100") {
		t.Fatal("valid loopback peers rejected")
	}
}
