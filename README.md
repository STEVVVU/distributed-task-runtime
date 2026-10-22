# Distributed task runtime

The [frozen benchmark](.agents/skills/verify-distributed-task-runtime/features/benchmark.md) drives the actual gRPC API and Go/Python workers. Use `python tools/benchmark.py smoke` for correctness, `python tools/benchmark.py benchmark` on an isolated Linux runner for five-repeat scaling, offered-load/stability and recovery cells, and `python tools/benchmark.py profile` for separate generator/runtime diagnostics. Raw source/environment, exact-result oracles, outcome/latency/resource receipts survive under ignored `.runs/benchmark-*/`. No measured performance result is published yet; service CPU hot-path profiling remains required before any optimization claim.

A small Go/Python distributed futures runtime for studying task DAGs, object locality and recovery. A Go coordinator persists lineage and control metadata. Separate Go worker processes schedule local execution and own their object stores. Python provides `remote`, `ObjectRef`, `put` and `get`. Services and Python clients communicate through real gRPC HTTP/2 using the standard protobuf `Struct` message.

This is an independently written teaching implementation inspired by [Stanford CS244B's Baby Ray capstone](https://www.scs.stanford.edu/24sp-cs244b/projects/Baby_Ray_Re-implementing_a_distributed_Python_runtime_in_Go.pdf), listed in the [official 2024 course program](https://www.scs.stanford.edu/24sp-cs244b/labs/program.html), and [Ray's OSDI 2018 paper](https://www.usenix.org/conference/osdi18/presentation/moritz). No student repository code or history is incorporated. The original contribution here is durable publication and attempt fencing with repeatable coordinator/worker crash journeys. This does not claim course completion, production suitability or a distributed performance speedup.

## Run the verified cluster

Requirements are Go 1.27 and Python 3.11. The dependency versions are in `go.mod`, `go.sum` and `requirements.lock`. On Windows PowerShell:

```powershell
python -m venv .venv
.venv/Scripts/python.exe -m pip install -r requirements.lock
go build -o bin/runtime.exe ./cmd/runtime
.venv/Scripts/python.exe tools/verify.py
```

The driver owns isolated loopback ports, a temporary capability token, two worker stores and a coordinator database. It runs actual Python tasks, kills an assigned worker, restarts the coordinator, asserts the results and stops its process trees. It prints the retained `.runs/<run>/evidence.json` path. Evidence and process logs survive cleanup and are intentionally ignored by Git. The maintained [verification skill](.agents/skills/verify-distributed-task-runtime/SKILL.md) and [feature map](.agents/skills/verify-distributed-task-runtime/features/README.md) document these journeys.

For a manually managed cluster, set `TASK_RUNTIME_TOKEN` to a random capability of at least 24 characters in each process. Start one coordinator and two workers from this repository root, using the same Python environment with the same installed trusted functions:

```powershell
bin/runtime.exe -role coordinator -address 127.0.0.1:7100 -data .runs/manual/coordinator.db
bin/runtime.exe -role worker -id session-a -address 127.0.0.1:7101 -coordinator 127.0.0.1:7100 -data .runs/manual/a.db -python .venv/Scripts/python.exe
bin/runtime.exe -role worker -id session-b -address 127.0.0.1:7102 -coordinator 127.0.0.1:7100 -data .runs/manual/b.db -python .venv/Scripts/python.exe
```

Run each command in its own terminal. A worker ID names one object-store session; never reuse it for a different store. Restart the coordinator with its same database file. Start fresh clusters with fresh databases. The driver does not touch manually started processes.

```python
import os
from examples.functions import add, total
from taskruntime import Runtime

runtime = Runtime("127.0.0.1:7100", os.environ["TASK_RUNTIME_TOKEN"])
root = runtime.put(10)
left = runtime.remote(add).remote(root, 2)
right = runtime.remote(add).remote(root, 3)
joined = runtime.remote(total).remote([left, right], offset=5)
assert runtime.get(joined) == 30
```

Use `-modules package.functions,other.trusted_module` on coordinator and workers to allow your installed modules. The Python API validates call signatures before submission. Functions must be named top-level functions defined in the allowlisted module. Closures, source upload, pickle, eval, arbitrary imports requested by clients, actors, GPUs and ML frameworks are outside this runtime.

## How it works

`internal/cluster/coordinator.go` owns the task state machine. Submission creates an immutable task definition and output reference in one bbolt transaction. References must already exist, which prevents forward references and cycles. Nested list/dictionary arguments are scanned for dependencies. A task is dispatched only when every parent has an available object location or durable root input. Parent failures propagate to children.

Each worker polls for work and sends independent heartbeats every 300 ms. The coordinator chooses the idle worker with the most dependency objects, breaking ties by worker ID. A worker runs one task at a time. It resolves arguments from its bbolt object cache, fetches missing computed payloads directly from peer workers over gRPC, then invokes the trusted Python function in a child interpreter. Root cache keys use `root:<id>` and task outputs use `task:<id>:<attempt>`; legal IDs cannot contain `:`, so the namespaces and attempts cannot overlap. Computed payloads never transit the coordinator. The worker commits output bytes locally before announcing completion. Clients fetch accepted computed results directly from the owning worker.

Root inputs are deliberately retained in the coordinator's bbolt database and included in assignments. This enables lineage reconstruction after every worker store is lost. Root dispatch bytes are not counted as peer transfers. The `transfers` counter counts successful peer object fetches; it demonstrates placement behavior without claiming a speedup. The coordinator does not track opportunistic cache replicas, so placement uses accepted output ownership and may miss a cached copy.

The coordinator durably commits task attempt, assignment and state before replying. A completion must match the current task ID, attempt and worker session. A replay of an accepted completion is idempotent. Losing a worker heartbeat for two seconds removes its locations and requeues running tasks and lost completed outputs. Tasks get at most three attempts, a 15-second assignment lease, a 10-second worker execution budget, and an 8-second Python subprocess budget. Queued lineage expires after 60 seconds from submission. Failures terminate explicitly; `get` also has its own bounded deadline.

Coordinator restart reloads roots, lineage and accepted output locations. It requeues previously running tasks and rejects their old completions. Persisted nodes get a two-second heartbeat grace period, allowing accepted results on surviving stores to remain readable. This is one durable coordinator, not a replicated control plane or consensus system. Loss of its database is unrecoverable. Store loss after the three-attempt budget or 60-second queued-lineage lifetime ends in a reported failure.

## Boundaries and limits

- Trusted local teaching workloads only. Every service binds to a numeric loopback address and requires the shared capability token. Loopback transport is plaintext; possession of the token grants the same trusted cluster role. This is not a sandbox for hostile code or clients.
- Retry only deterministic pure tasks. Execution can happen more than once, including after coordinator restart. Side effects are not exactly once.
- JSON values only. Protobuf Struct uses binary64 numbers, so values must be finite and within `2**53 - 1`. Integer/float identity, tuples and custom Python types are not preserved.
- Objects and root batches are limited to 1 MiB, RPC messages to 2 MiB, task specifications to 256 KiB, argument/value nesting to 32 and task dependencies to 128. Objects must also fit a protobuf response; task submissions check their serialized assignment with reserved room for bounded owner metadata. Protobuf may be larger than JSON, so a batch within the JSON limit can still be rejected before acceptance. Replies are serialized and checked before metadata commits. A cluster accepts at most 10,000 tasks and 10,000 roots. `Status` is a teaching snapshot without pagination; sufficiently large metadata snapshots can exceed the RPC cap and return an explicit size error.
- Worker stores retain attempt objects for the cluster lifetime. There is no garbage collector, spill policy, cancellation API, dynamic code distribution, node capacity model or multi-host support. Use isolated scratch databases for bounded exercises.
- Python children execute with the worker's user permissions. The verification driver terminates its owned Windows process trees; operators must similarly stop a worker's children when manually forcing it down.

## Checks

```powershell
go test ./...
go vet ./...
go build -o bin/runtime.exe ./cmd/runtime
.venv/Scripts/python.exe -m unittest discover -s tests -v
.venv/Scripts/python.exe tools/verify.py
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
.venv/Scripts/python.exe -m pip_audit -r requirements.lock
```

The optional Python audit tool used during development is `pip-audit==2.10.1`; install it in the venv when rerunning that check. It is not required by the runtime. Go's race detector needs a C compiler/CGO and was unavailable in the Windows validation environment. Build, vet, focused state-machine tests, Python tests and the real process driver are separate checks.

`.github/workflows/ci.yml` runs those checks on Windows with Go 1.27.3 and Python 3.11, then retains `.runs/` evidence for seven days, including failed runs. Official GitHub actions are pinned by commit SHA and the workflow has read-only repository permissions. The workflow was checked locally with `actionlint v1.7.11` and its product commands were executed locally; a hosted CI run requires publication and has not been triggered from this checkout.

Dependency provenance and the security update are recorded in [THIRD_PARTY.md](THIRD_PARTY.md). Source is MIT licensed.
