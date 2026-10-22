# Native profile preparation and future k6 adapter

This source-only preparation closes a diagnostic gap; no profile result or product optimization is claimed. Main must review the actual diff, build/test both variants on the hosted runner and inspect raw profiles before attributing the overload.

## Frozen diagnostic cell

The original source `0a9aa3f67c1d39f489bc074a9a695094c4ce6bf6`, run `39729586066`, offered 160 DAGs at 16 DAG/s over ten seconds: 97 correct finishes, 63 generator declines, no product failures/rejections/timeouts. The profile reuses that exact fixture: two workers, 4,096 object items, seed 421, 20 ms installed `slow` task, parent/child `slow` → `total` DAG, concurrency 32 and one untimed warm DAG. Its JSON input is 15,961 bytes, SHA256 `2804d6361162e3f4d958ffa8ae7aeaea8a19fa3e255bd0396bba96b8dc5a08c6`, sum 196,616. Every completed child must return that sum plus its offered index. The existing post-result status/task-attempt and peer-transfer proofs remain part of the same path.

Normal builds use a no-op profiling helper. Only `-tags benchmark` enables `TASK_RUNTIME_PROFILE_ADDR`; the driver supplies a fresh numeric loopback address per owned native process. Existing `wire.Loopback` rejects public/wildcard/DNS/zero-port addresses, and listener failure returns synchronously before runtime state opens. A private HTTP mux exposes only GET `/debug/pprof/profile`, accepting 1–60 seconds. It does not serve cmdline, heap, symbols, debug indexes or the default mux. Existing production gRPC capability/token, persistence-before-response, fencing and signal behavior remain intact.

Reproduce after the normal environment checks:

```sh
go mod verify
go test ./...
go vet ./...
go build -o bin/runtime ./cmd/runtime
go test -tags benchmark ./cmd/runtime
go vet -tags benchmark ./cmd/runtime
go build -tags benchmark -o bin/runtime-profile ./cmd/runtime
python -m unittest discover -s tools -p test_benchmark.py -v
python tools/benchmark.py smoke
python tools/benchmark.py profile
```

The manual benchmark workflow builds the separate tagged binary only for its existing `profile` input; normal smoke still drives the normal binary. The new `matched` input independently builds the frozen baseline source and candidate source as normal binaries with the same Go 1.27.3 production flags and distinct `GOCACHE` directories. It smoke-tests each binary, runs `tools/verify.py` on the candidate, then runs five interleaved pairs of the exact 16 DAG/s overload fixture. Candidate runs first on even repeats and baseline first on odd repeats. Every trial retains source SHA, executable SHA256, deterministic input hash, all offers and outcomes, latency tails, process resources and cleanup status. Non-green workload outcomes do not stop later pairs; process/driver failures or cleanup failures do. These runs provide matched evidence only and do not assert a speedup.

The profile driver requests three native CPU profiles concurrently after warmup, using `GET http://127.0.0.1:<owned-port>/debug/pprof/profile?seconds=20`. Each capture is bounded by a 25-second HTTP timeout and an 8 MiB response limit. All three gzip/nonempty files, capture-thread completion and normal cleanup are required. Captures finish before cleanup; a failed load remains failed even when profile capture succeeds. `native_profiles_passed` is separate from the workload verdict. The normal failed offered-load outcomes are never erased or replaced with closed-loop timing.

Twenty-second profile windows cover the original approximately 13.5-second offered/drain observation, but a new unusually long drain could outlast them. Record actual CPU-profile timestamps alongside retained load start/end before claiming temporal coverage. Profile receipts retain native binary SHA256, request URL, byte count/hash and request/response boundaries. Generator cProfile is retained even on driver error; its existing main-thread scope is distinct from process CPU. Scheduler/GC traces remain separate. Profile timings are excluded from comparison samples.

Linux profile resource capture also enumerates descendants from **all** native OS threads, validates observed parent PID and records start-time ticks for identity. Short Python executors can disappear between 100 ms samples, so native `/proc` reaped-child CPU ticks are retained separately. Do not sum reaped-child counters and overlapping live-child samples into an unsupported exact aggregate; short-lived RSS peaks can still be missed. No PID outside the owned tree is stopped or signaled.

Exact artifact allowlist:

```
.runs/benchmark-*/summary.json
.runs/benchmark-*/trial-*/result.json
.runs/benchmark-*/trial-*/resources.json
.runs/benchmark-*/trial-*/*.log
.runs/benchmark-*/trial-*/*.pprof
.runs/benchmark-*/*.pstats
```

Databases, configs, capabilities, raw environment dumps and arbitrary object-store files remain excluded. Generated profiles are ignored run data. No dependency was added. Native Go checks, including loopback/occupied-listener/CPU-only-handler controls and the normal-build no-op control, are prepared for hosted execution; local validation is limited to seven pure Python controls, Python compilation, Go formatting and diff checks under the resource gate.

## Future native k6 gRPC adapter: plan only

Use the pinned k6 v2.3.0 native `k6/net/grpc` module; reuse the owned launcher and existing `protocol/runtime.proto`. No HTTP bridge, extension, generated client or alternate wire format is needed. k6 [loads proto descriptors in init](https://grafana.com/docs/k6/latest/javascript-api/k6-net-grpc/client/client-load/), and its [pinned loader supplies standard imports](https://github.com/grafana/k6/blob/v2.3.0/internal/js/modules/k6/grpc/client.go#L45-L85), including the existing Struct dependency. Unary [invoke uses Protobuf JSON mapping](https://grafana.com/docs/k6/latest/javascript-api/k6-net-grpc/client/client-invoke/); [Struct maps to a JSON object](https://protobuf.dev/programming-guides/json/), so requests use the existing operation dictionaries directly.

The shortest semantic adapter has one RPC wrapper: load `runtime.proto`, connect to the task-owned numeric loopback endpoint with plaintext HTTP/2, invoke `/runtime.Runtime/Call`, attach metadata `authorization: <raw capability>` exactly as `taskruntime/client.py` does (no Bearer prefix), enforce the existing per-call deadline, and close the connection per call to match the current SDK baseline. Set [native connect options](https://grafana.com/docs/k6/latest/javascript-api/k6-net-grpc/client/client-connect/) `maxReceiveSize` and `maxSendSize` to `2 << 20`; [pinned source](https://github.com/grafana/k6/blob/v2.3.0/internal/js/modules/k6/grpc/client.go#L267-L273) maps both to grpc-go call limits. A later connection-pooling study must explicitly version that policy instead of presenting it as a product speedup. No client extension or relaxed wire limit is needed.

Setup puts the shared deterministic root object outside timing. Each native `group('dag')` iteration submits a unique installed `examples.functions.slow` parent referencing the root, then a unique `total` child referencing that parent with literal offset. Mirror SDK argument shapes (`kind: ref`/`literal`), preserve the 20 ms argument, and retain IDs on uncertain outcomes. Poll `locate` under the existing 40-second DAG deadline and 50 ms polling policy; honor failed-task state, fetch a done object from its returned worker location using the same token, verify exact arithmetic, and retain the existing post-result status/task metadata call. This keeps the true SDK/domain path rather than timing submission alone. Never submit source, pickle or arbitrary modules.

k6 should own constant-arrival scheduling, VUs, native dropped iterations, `group_duration`, checks and raw summaries. Keep separate requested/native offer counts and the documented pinned boundary behavior, plus explicit failed/rejected/uncertain/pending outcomes. Preserve existing worker-loss/restart semantic oracles in the launcher. Before migration, a real two-worker semantic smoke must prove Struct encoding, capability rejection, exact parent/child results, pending identity handling and cleanup. This is a future, explicitly versioned generator study; no adapter code, new load engine, framework installation or measurement was added here.

## Captured overload profile and candidate

[Run 39750063564](https://github.com/STEVVVU/distributed-task-runtime/actions/runs/39750063564) passed normal and tagged Go tests/vet/builds and the real smoke. Its diagnostic cell retained 160 offers, 83 exact completions and 77 generator declines, with zero product failures, rejections or timeouts. All three native profiles passed hash and bounded-capture checks, and all owned processes and the sampler exited. The 20-second request/response windows covered the 13.162-second offered-load/drain interval; these are diagnostic observations, not matched throughput samples.

The coordinator profile sampled 8.96 CPU seconds. The per-request pre-mutation `json.Marshal(c.state)` accounted for 2.02 seconds, or 22.54% of sampled CPU. `save` accounted for 3.23 seconds cumulatively; these shares overlap other JSON totals. The candidate reuses the JSON bytes of the last successful bbolt commit for rollback, replacing that snapshot only after the next transaction succeeds. It preserves the same whole-state write and durable-before-response rule. Independent source review found no correctness regression, with hosted checks and matched measurement still required. Keeping one committed serialized state adds retained memory; resource receipts must establish that cost. No speedup is claimed before the paired run.
