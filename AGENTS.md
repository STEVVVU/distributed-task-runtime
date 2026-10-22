# Distributed task runtime

Go owns global task metadata in bbolt and worker-local scheduling/object stores. Python exposes remote futures and executes trusted installed functions. All transport is gRPC with protobuf Struct messages, loopback bindings, and a shared capability token.

Use `go test ./...`, `go vet ./...`, `go build -o bin/runtime.exe ./cmd/runtime`, and `.venv/Scripts/python.exe tools/verify.py`. Format Go with `gofmt -w cmd internal`.

Read `.agents/skills/verify-distributed-task-runtime/SKILL.md` before changing runtime behavior. Preserve the bounded retry, publication fencing, durable-before-response, and object-location invariants. Never add pickle, eval, arbitrary source submission, public bindings, or claims of exactly-once side effects.
