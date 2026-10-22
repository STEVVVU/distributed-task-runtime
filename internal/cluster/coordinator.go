package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"distributed-task-runtime/internal/wire"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"
)

type Coordinator struct {
	mu        sync.Mutex
	db        *bolt.DB
	state     State
	modules   map[string]bool
	committed []byte
}

func OpenCoordinator(path, modules string) (*Coordinator, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: time.Second})
	if err != nil {
		return nil, err
	}
	c := &Coordinator{db: db, modules: map[string]bool{}}
	for _, m := range strings.Split(modules, ",") {
		c.modules[m] = true
	}
	c.state = State{Tasks: map[string]*Task{}, Objects: map[string]*Object{}, Nodes: map[string]*Node{}}
	err = db.Update(func(tx *bolt.Tx) error {
		b, e := tx.CreateBucketIfNotExists([]byte("runtime"))
		if e != nil {
			return e
		}
		if data := b.Get([]byte("state")); data != nil {
			return json.Unmarshal(data, &c.state)
		}
		return nil
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	for _, n := range c.state.Nodes {
		n.Last = time.Now().UnixMilli()
		n.Busy = ""
	}
	for _, t := range c.state.Tasks {
		if t.State == "running" {
			c.retry(t, "coordinator restarted")
		}
	}
	if err = c.save(c.state); err != nil {
		db.Close()
		return nil, err
	}
	return c, nil
}
func (c *Coordinator) Close() error { return c.db.Close() }
func (c *Coordinator) save(s State) error {
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	if err := c.db.Update(func(tx *bolt.Tx) error { return tx.Bucket([]byte("runtime")).Put([]byte("state"), b) }); err != nil {
		return err
	}
	c.committed = b
	return nil
}
func (c *Coordinator) retry(t *Task, reason string) {
	t.Worker = ""
	if t.Attempt >= MaxAttempts {
		t.State = "failed"
		t.Error = "attempt limit: " + reason
	} else {
		t.State = "queued"
		t.Error = ""
	}
}
func (c *Coordinator) expire() {
	now := time.Now().UnixMilli()
	for id, n := range c.state.Nodes {
		if now-n.Last > 2000 {
			delete(c.state.Nodes, id)
			for _, o := range c.state.Objects {
				delete(o.Locations, id)
			}
			for _, t := range c.state.Tasks {
				if t.State == "running" && t.Worker == id {
					c.retry(t, "worker heartbeat lost")
				}
			}
		}
	}
	for _, t := range c.state.Tasks {
		if t.State == "running" && now-t.Started > 15000 {
			if n := c.state.Nodes[t.Worker]; n != nil {
				n.Busy = ""
			}
			c.retry(t, "task lease expired")
		}
		if t.State == "done" && len(c.state.Objects[t.ID].Locations) == 0 {
			c.retry(t, "output store lost")
		}
	}
	for _, t := range c.state.Tasks {
		if t.State == "queued" && now-t.Created > 60000 {
			t.State = "failed"
			t.Error = "task deadline exceeded"
		}
	}
}
func (c *Coordinator) Run(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			c.mu.Lock()
			c.expire()
			if err := c.save(c.state); err != nil {
				log.Printf("metadata expiration commit failed: %v", err)
				c.state = State{}
				_ = json.Unmarshal(c.committed, &c.state)
			}
			c.mu.Unlock()
		}
	}
}
func (c *Coordinator) Call(ctx context.Context, m map[string]any) (any, error) {
	var r Request
	if err := wire.Decode(m, &r); err != nil {
		return nil, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	// Durable state changes become visible only after bbolt commits successfully.
	out, err := c.handle(r)
	snapshot, encodeErr := wire.Message(out)
	if err == nil {
		err = encodeErr
	}
	if err == nil && proto.Size(snapshot) > wire.MaxMessage {
		err = errors.New("reply exceeds protobuf RPC budget")
	}
	if err == nil {
		err = c.save(c.state)
	}
	if err != nil {
		c.state = State{}
		_ = json.Unmarshal(c.committed, &c.state)
		return nil, err
	}
	return snapshot.AsMap(), nil
}
func (c *Coordinator) handle(r Request) (any, error) {
	c.expire()
	switch r.Op {
	case "status":
		objects := map[string]any{}
		for id, o := range c.state.Objects {
			objects[id] = map[string]any{"id": id, "root_bytes": len(o.Root), "locations": o.Locations}
		}
		return map[string]any{"tasks": c.state.Tasks, "objects": objects, "nodes": c.state.Nodes}, nil
	case "put":
		if !identifier.MatchString(r.ID) || len(r.Value) == 0 || len(r.Value) > wire.MaxObject {
			return nil, errors.New("invalid root object")
		}
		if err := wire.ValidateObject(r.Value); err != nil {
			return nil, err
		}
		var value any
		if err := json.Unmarshal(r.Value, &value); err != nil {
			return nil, err
		}
		if err := validValue(value, 0); err != nil {
			return nil, err
		}
		if old := c.state.Objects[r.ID]; old != nil {
			if !reflect.DeepEqual(old.Root, r.Value) {
				return nil, errors.New("object ID conflict")
			}
			return map[string]any{"id": r.ID}, nil
		}
		if len(c.state.Objects) >= 10000 {
			return nil, errors.New("object capacity reached")
		}
		c.state.Objects[r.ID] = &Object{ID: r.ID, Root: r.Value, Locations: map[string]Location{}}
		return map[string]any{"id": r.ID}, nil
	case "submit":
		t := r.Task
		if t == nil || !identifier.MatchString(t.ID) || !c.modules[t.Module] || !symbol.MatchString(t.Function) {
			return nil, errors.New("invalid task or module not allowlisted")
		}
		encoded, _ := json.Marshal(t)
		if len(encoded) > 256<<10 {
			return nil, errors.New("task specification exceeds 256 KiB")
		}
		deps := map[string]bool{}
		for _, a := range t.Args {
			if err := dependencies(a, deps, 0); err != nil {
				return nil, err
			}
		}
		for _, a := range t.Kwargs {
			if err := dependencies(a, deps, 0); err != nil {
				return nil, err
			}
		}
		for id := range deps {
			if c.state.Objects[id] == nil {
				return nil, fmt.Errorf("unknown dependency %s", id)
			}
		}
		t.Deps = nil
		for id := range deps {
			t.Deps = append(t.Deps, id)
		}
		sort.Strings(t.Deps)
		if len(t.Deps) > 128 {
			return nil, errors.New("task exceeds 128 dependencies")
		}
		rootBytes := 0
		for _, id := range t.Deps {
			rootBytes += len(c.state.Objects[id].Root)
		}
		if rootBytes > wire.MaxObject {
			return nil, errors.New("root input batch exceeds 1 MiB")
		}
		if old := c.state.Tasks[t.ID]; old != nil {
			if old.Module != t.Module || old.Function != t.Function || !reflect.DeepEqual(old.Args, t.Args) || !reflect.DeepEqual(old.Kwargs, t.Kwargs) {
				return nil, errors.New("task ID conflict")
			}
			return map[string]any{"id": t.ID}, nil
		}
		if c.state.Objects[t.ID] != nil || len(c.state.Tasks) >= 10000 {
			return nil, errors.New("task capacity or ID conflict")
		}
		t.State = "queued"
		t.Attempt = 0
		t.Worker = ""
		t.Error = ""
		t.Created = time.Now().UnixMilli()
		inputs := map[string]Spec{}
		for _, id := range t.Deps {
			inputs[id] = Spec{Root: c.state.Objects[id].Root, Locations: map[string]Location{}}
		}
		candidate, err := wire.Message(Assignment{Task: t, Inputs: inputs})
		if err != nil {
			return nil, err
		}
		// Reserve one bounded owner location per dependency and assignment metadata.
		if proto.Size(candidate)+1024*(len(t.Deps)+1) > wire.MaxMessage {
			return nil, errors.New("assignment exceeds protobuf RPC budget")
		}
		c.state.Tasks[t.ID] = t
		c.state.Objects[t.ID] = &Object{ID: t.ID, Locations: map[string]Location{}}
		return map[string]any{"id": t.ID}, nil
	case "locate":
		o := c.state.Objects[r.ID]
		if o == nil {
			return nil, errors.New("unknown object")
		}
		state := "done"
		message := ""
		if t := c.state.Tasks[r.ID]; t != nil {
			state = t.State
			message = t.Error
		}
		return map[string]any{"state": state, "error": message, "object": o}, nil
	case "heartbeat":
		if !identifier.MatchString(r.Worker) || !wire.Loopback(r.Address) {
			return nil, errors.New("invalid worker registration")
		}
		n := c.state.Nodes[r.Worker]
		if n == nil {
			n = &Node{ID: r.Worker, Address: r.Address}
			c.state.Nodes[r.Worker] = n
		} else if n.Address != r.Address {
			return nil, errors.New("worker address conflict")
		}
		n.Last = time.Now().UnixMilli()
		return map[string]any{"ok": true}, nil
	case "poll":
		n := c.state.Nodes[r.Worker]
		if n == nil {
			return nil, errors.New("worker not registered")
		}
		if n.Busy != "" {
			return Assignment{}, nil
		}
		ids := make([]string, 0, len(c.state.Tasks))
		for id := range c.state.Tasks {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		for _, id := range ids {
			t := c.state.Tasks[id]
			if t.State != "queued" {
				continue
			}
			ready := true
			for _, dep := range t.Deps {
				if parent := c.state.Tasks[dep]; parent != nil && parent.State == "failed" {
					t.State = "failed"
					t.Error = "dependency failed: " + dep
					ready = false
					break
				}
				o := c.state.Objects[dep]
				if len(o.Root) == 0 && len(o.Locations) == 0 {
					ready = false
				}
			}
			if !ready {
				continue
			}
			best := ""
			score := -1
			for wid, node := range c.state.Nodes {
				if node.Busy != "" {
					continue
				}
				local := 0
				for _, dep := range t.Deps {
					if _, ok := c.state.Objects[dep].Locations[wid]; ok {
						local++
					}
				}
				if local > score || (local == score && (best == "" || wid < best)) {
					best = wid
					score = local
				}
			}
			if best != r.Worker {
				continue
			}
			t.Attempt++
			t.State = "running"
			t.Started = time.Now().UnixMilli()
			t.Worker = r.Worker
			n.Busy = t.ID
			inputs := map[string]Spec{}
			for _, dep := range t.Deps {
				o := c.state.Objects[dep]
				inputs[dep] = Spec{Root: o.Root, Locations: o.Locations}
			}
			return Assignment{Task: t, Inputs: inputs}, nil
		}
		return Assignment{}, nil
	case "complete":
		if r.Attempt < 1 || r.Transfers < 0 || len(r.Error) > 4096 {
			return nil, errors.New("invalid completion")
		}
		t := c.state.Tasks[r.ID]
		if t == nil {
			return nil, errors.New("unknown task")
		}
		if t.Attempt != r.Attempt || t.Worker != r.Worker {
			return nil, errors.New("stale attempt rejected")
		}
		if (t.State == "done" || t.State == "failed") && t.Error == r.Error {
			return map[string]any{"accepted": true, "duplicate": true}, nil
		}
		n := c.state.Nodes[r.Worker]
		if t.State != "running" || n == nil {
			return nil, errors.New("stale attempt rejected")
		}
		n.Busy = ""
		n.Executed++
		n.Transfers += r.Transfers
		if r.Retry && r.Error != "" {
			c.retry(t, r.Error)
		} else if r.Error != "" {
			t.State = "failed"
			t.Error = r.Error
		} else {
			t.State = "done"
			c.state.Objects[t.ID].Locations[r.Worker] = Location{Address: n.Address, Key: taskObjectKey(t.ID, t.Attempt)}
		}
		return map[string]any{"accepted": true}, nil
	default:
		return nil, errors.New("unknown coordinator operation")
	}
}
