package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"distributed-task-runtime/internal/wire"
	bolt "go.etcd.io/bbolt"
)

type Worker struct {
	ID, Address, Coordinator, Token, Python, Modules string
	DB                                               *bolt.DB
}

func (w *Worker) Open(path string) error {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return err
	}
	w.DB = db
	return db.Update(func(tx *bolt.Tx) error { _, err := tx.CreateBucketIfNotExists([]byte("objects")); return err })
}
func (w *Worker) load(key string) (json.RawMessage, error) {
	var data []byte
	err := w.DB.View(func(tx *bolt.Tx) error {
		value := tx.Bucket([]byte("objects")).Get([]byte(key))
		if value == nil {
			return errors.New("object absent")
		}
		data = bytes.Clone(value)
		return nil
	})
	return data, err
}
func (w *Worker) store(key string, value json.RawMessage) error {
	if len(value) > wire.MaxObject || !json.Valid(value) {
		return errors.New("invalid or oversized object")
	}
	return w.DB.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("objects")).Put([]byte(key), value) })
}
func (w *Worker) Call(ctx context.Context, m map[string]any) (any, error) {
	var r Request
	if err := wire.Decode(m, &r); err != nil {
		return nil, err
	}
	if r.Op != "fetch" {
		return nil, errors.New("worker exposes only fetch")
	}
	value, err := w.load(r.Key)
	if err != nil {
		return nil, err
	}
	return map[string]any{"value": value}, nil
}
func (w *Worker) Run(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(300 * time.Millisecond)
		defer ticker.Stop()
		for {
			var response map[string]any
			_ = wire.Call(ctx, w.Coordinator, w.Token, Request{Op: "heartbeat", Worker: w.ID, Address: w.Address}, &response)
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()
	for ctx.Err() == nil {
		var assignment Assignment
		err := wire.Call(ctx, w.Coordinator, w.Token, Request{Op: "poll", Worker: w.ID}, &assignment)
		if err != nil || assignment.Task == nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
			}
			continue
		}
		transfers, taskErr := w.execute(ctx, assignment)
		request := Request{Op: "complete", ID: assignment.Task.ID, Attempt: assignment.Task.Attempt, Worker: w.ID, Transfers: transfers}
		if taskErr != nil {
			request.Error = taskErr.Error()
			request.Retry = strings.HasPrefix(request.Error, "input ")
		}
		// A response can be lost after durable acceptance; replay this exact completion.
		for tries := 0; tries < 20 && ctx.Err() == nil; tries++ {
			var reply map[string]any
			err = wire.Call(ctx, w.Coordinator, w.Token, request, &reply)
			if err == nil || strings.Contains(err.Error(), "stale attempt") {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
	}
}
func (w *Worker) execute(ctx context.Context, a Assignment) (int, error) {
	ctx, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	values := map[string]any{}
	transfers := 0
	for id, spec := range a.Inputs {
		key := "root-" + id
		if len(spec.Root) == 0 {
			for _, loc := range spec.Locations {
				key = loc.Key
				break
			}
		}
		data, err := w.load(key)
		if err != nil && len(spec.Root) > 0 {
			data = spec.Root
			err = w.store(key, data)
		} else if err != nil {
			for _, loc := range spec.Locations {
				var response struct {
					Value json.RawMessage `json:"value"`
				}
				err = wire.Call(ctx, loc.Address, w.Token, Request{Op: "fetch", Key: loc.Key}, &response)
				if err == nil {
					data = response.Value
					transfers++
					err = w.store(key, data)
					break
				}
			}
		}
		if err != nil {
			return transfers, fmt.Errorf("input %s unavailable: %w", id, err)
		}
		var value any
		if err = json.Unmarshal(data, &value); err != nil {
			return transfers, err
		}
		values[id] = value
	}
	args := make([]any, 0, len(a.Task.Args))
	for _, arg := range a.Task.Args {
		args = append(args, resolve(arg, values))
	}
	kwargs := map[string]any{}
	for key, arg := range a.Task.Kwargs {
		kwargs[key] = resolve(arg, values)
	}
	request := map[string]any{"module": a.Task.Module, "function": a.Task.Function, "args": args, "kwargs": kwargs, "modules": strings.Split(w.Modules, ",")}
	data, err := json.Marshal(request)
	if err != nil {
		return transfers, err
	}
	taskCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	command := exec.CommandContext(taskCtx, w.Python, "-m", "taskruntime.worker_exec")
	command.Stdin = bytes.NewReader(data)
	var stdout, stderr boundedBuffer
	command.Stdout = &stdout
	command.Stderr = &stderr
	if err = command.Run(); err != nil {
		if taskCtx.Err() != nil {
			return transfers, errors.New("task execution deadline exceeded")
		}
		return transfers, fmt.Errorf("python task failed: %s", stderr.String())
	}
	var result struct {
		Value json.RawMessage `json:"value"`
		Error string          `json:"error"`
	}
	if err = json.Unmarshal(stdout.Bytes(), &result); err != nil {
		return transfers, errors.New("invalid Python result")
	}
	if result.Error != "" {
		return transfers, errors.New(result.Error)
	}
	return transfers, w.store(fmt.Sprintf("%s-%d", a.Task.ID, a.Task.Attempt), result.Value)
}
func resolve(a Arg, values map[string]any) any {
	switch a.Kind {
	case "ref":
		return values[a.Ref]
	case "list":
		out := make([]any, 0, len(a.Items))
		for _, child := range a.Items {
			out = append(out, resolve(child, values))
		}
		return out
	case "dict":
		out := map[string]any{}
		for key, child := range a.Fields {
			out[key] = resolve(child, values)
		}
		return out
	default:
		return a.Value
	}
}

type boundedBuffer struct{ bytes.Buffer }

func (b *boundedBuffer) Write(data []byte) (int, error) {
	if b.Len()+len(data) > wire.MaxObject {
		return 0, errors.New("Python output exceeds object limit")
	}
	return b.Buffer.Write(data)
}
