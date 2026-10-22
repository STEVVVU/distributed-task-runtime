package cluster

import (
	"context"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"distributed-task-runtime/internal/wire"
)

func call(t *testing.T, c *Coordinator, r Request) any {
	t.Helper()
	m, err := wire.Message(r)
	if err != nil {
		t.Fatal(err)
	}
	result, err := c.Call(context.Background(), m.AsMap())
	if err != nil {
		t.Fatal(err)
	}
	return result
}
func setup(t *testing.T) (*Coordinator, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metadata.db")
	c, err := OpenCoordinator(path, "examples.functions")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, path
}
func assign(t *testing.T, c *Coordinator, id string) *Task {
	t.Helper()
	call(t, c, Request{Op: "heartbeat", Worker: "a", Address: "127.0.0.1:12345"})
	call(t, c, Request{Op: "submit", Task: &Task{ID: id, Module: "examples.functions", Function: "add", Args: []Arg{{Kind: "literal", Value: float64(1)}, {Kind: "literal", Value: float64(2)}}, Kwargs: map[string]Arg{}}})
	call(t, c, Request{Op: "poll", Worker: "a"})
	task := *c.state.Tasks[id]
	return &task
}
func TestDurableLineageFencesOldAssignment(t *testing.T) {
	c, path := setup(t)
	old := assign(t, c, "task")
	call(t, c, Request{Op: "put", ID: "root", Value: json.RawMessage(`{"answer":42}`)})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	resumed, err := OpenCoordinator(path, "examples.functions")
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	if resumed.state.Tasks[old.ID].State != "queued" {
		t.Fatal("running task not recovered")
	}
	message, _ := wire.Message(Request{Op: "complete", ID: old.ID, Worker: old.Worker, Attempt: old.Attempt})
	if _, err = resumed.Call(context.Background(), message.AsMap()); err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("old completion accepted: %v", err)
	}
	call(t, resumed, Request{Op: "poll", Worker: "a"})
	newTask := resumed.state.Tasks[old.ID]
	if newTask.Attempt != old.Attempt+1 {
		t.Fatal("attempt not increased durably")
	}
	completion := Request{Op: "complete", ID: old.ID, Worker: "a", Attempt: newTask.Attempt}
	call(t, resumed, completion)
	call(t, resumed, completion)
	if resumed.state.Nodes["a"].Executed != 1 {
		t.Fatal("duplicate completion counted twice")
	}
	if err = resumed.Close(); err != nil {
		t.Fatal(err)
	}
	accepted, err := OpenCoordinator(path, "examples.functions")
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	if accepted.state.Tasks[old.ID].State != "done" || string(accepted.state.Objects["root"].Root) != `{"answer":42}` {
		t.Fatal("accepted publication/root lost")
	}
}
func TestBoundedLeaseAndDependencyFailure(t *testing.T) {
	c, _ := setup(t)
	assign(t, c, "parent")
	call(t, c, Request{Op: "submit", Task: &Task{ID: "child", Module: "examples.functions", Function: "add", Args: []Arg{{Kind: "ref", Ref: "parent"}, {Kind: "literal", Value: float64(1)}}}})
	for attempt := 1; attempt <= MaxAttempts; attempt++ {
		task := c.state.Tasks["parent"]
		if task.Attempt != attempt {
			t.Fatalf("attempt %d != %d", task.Attempt, attempt)
		}
		task.Started = time.Now().Add(-16 * time.Second).UnixMilli()
		c.expire()
		if attempt < MaxAttempts {
			call(t, c, Request{Op: "poll", Worker: "a"})
		}
	}
	if c.state.Tasks["parent"].State != "failed" {
		t.Fatal("attempt budget not enforced")
	}
	call(t, c, Request{Op: "poll", Worker: "a"})
	if c.state.Tasks["child"].State != "failed" || !strings.Contains(c.state.Tasks["child"].Error, "dependency failed") {
		t.Fatal("dependency failure did not propagate")
	}
}
func TestWorkerLossReconstructsOutput(t *testing.T) {
	c, _ := setup(t)
	task := assign(t, c, "task")
	call(t, c, Request{Op: "complete", ID: task.ID, Worker: "a", Attempt: task.Attempt})
	c.state.Nodes["a"].Last = time.Now().Add(-3 * time.Second).UnixMilli()
	c.expire()
	if c.state.Tasks[task.ID].State != "queued" || len(c.state.Objects[task.ID].Locations) != 0 {
		t.Fatal("lost output not invalidated for reconstruction")
	}
}
func TestBoundaryValidationAndStoreDurability(t *testing.T) {
	for _, arg := range []Arg{{Kind: "unknown"}, {Kind: "ref", Ref: "bad/id"}, {Kind: "literal", Value: 1e100}, {Kind: "ref", Ref: "okay", Items: []Arg{{Kind: "literal"}}}} {
		if dependencies(arg, map[string]bool{}, 0) == nil {
			t.Fatalf("accepted malformed argument %+v", arg)
		}
	}
	path := filepath.Join(t.TempDir(), "worker.db")
	w := &Worker{}
	if err := w.Open(path); err != nil {
		t.Fatal(err)
	}
	if err := w.store(taskObjectKey("task", 1), json.RawMessage(`{"x":[1,2]}`)); err != nil {
		t.Fatal(err)
	}
	w.DB.Close()
	if err := w.Open(path); err != nil {
		t.Fatal(err)
	}
	defer w.DB.Close()
	data, err := w.load(taskObjectKey("task", 1))
	if err != nil || string(data) != `{"x":[1,2]}` {
		t.Fatalf("worker object did not survive reopen: %s %v", data, err)
	}
}

func TestFailedCommitDoesNotPublish(t *testing.T) {
	c, path := setup(t)
	call(t, c, Request{Op: "put", ID: "accepted", Value: json.RawMessage(`{"answer":42}`)})
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	message, _ := wire.Message(Request{Op: "put", ID: "must-not-exist", Value: json.RawMessage(`1`)})
	if _, err := c.Call(context.Background(), message.AsMap()); err == nil {
		t.Fatal("closed database accepted publication")
	}
	if c.state.Objects["must-not-exist"] != nil {
		t.Fatal("failed commit became visible")
	}
	if c.state.Objects["accepted"] == nil || string(c.state.Objects["accepted"].Root) != `{"answer":42}` {
		t.Fatal("failed commit lost previously committed state")
	}
	reopened, err := OpenCoordinator(path, "examples.functions")
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.state.Objects["must-not-exist"] != nil || string(reopened.state.Objects["accepted"].Root) != `{"answer":42}` {
		t.Fatal("rollback disagrees with durable state after reopen")
	}
}

func TestRootAndTaskStoreNamespacesDoNotOverlap(t *testing.T) {
	w := &Worker{}
	if err := w.Open(filepath.Join(t.TempDir(), "worker.db")); err != nil {
		t.Fatal(err)
	}
	defer w.DB.Close()
	rootKey, taskKey := rootObjectKey("x-1"), taskObjectKey("root-x", 1)
	if err := w.store(taskKey, json.RawMessage(`99`)); err != nil {
		t.Fatal(err)
	}
	if err := w.store(rootKey, json.RawMessage(`10`)); err != nil {
		t.Fatal(err)
	}
	for key, want := range map[string]string{rootKey: "10", taskKey: "99"} {
		value, err := w.load(key)
		if err != nil || string(value) != want {
			t.Fatalf("key %q returned %s, want %s: %v", key, value, want, err)
		}
	}
}

func TestProtobufAssignmentBudgetRejectsBeforePublication(t *testing.T) {
	c, _ := setup(t)
	args := make([]Arg, 4)
	value := json.RawMessage("[" + strings.Repeat("0,", 49999) + "0]")
	for i, id := range []string{"one", "two", "three", "four"} {
		call(t, c, Request{Op: "put", ID: id, Value: value})
		args[i] = Arg{Kind: "ref", Ref: id}
	}
	message, _ := wire.Message(Request{Op: "submit", Task: &Task{ID: "oversized", Module: "examples.functions", Function: "slow", Args: []Arg{{Kind: "list", Items: args}}}})
	if _, err := c.Call(context.Background(), message.AsMap()); err == nil || !strings.Contains(err.Error(), "assignment exceeds protobuf RPC budget") {
		t.Fatalf("oversized assignment accepted: %v", err)
	}
	if c.state.Tasks["oversized"] != nil || c.state.Objects["oversized"] != nil {
		t.Fatal("rejected assignment was published")
	}
}
