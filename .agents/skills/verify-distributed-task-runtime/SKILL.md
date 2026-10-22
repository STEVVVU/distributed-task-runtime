---
name: verify-distributed-task-runtime
description: Launch and drive the real Go coordinator, two Go workers and Python remote API, including worker loss and coordinator restart.
---

# Launch

Build `go build -o bin/runtime.exe ./cmd/runtime`. Install the pinned `requirements.lock` in `.venv`. Run `.venv/Scripts/python.exe tools/verify.py`. The maintained driver owns unique loopback ports, a temporary capability token, process handles and isolated `.runs/<timestamp>/` state. It starts the coordinator then two worker processes. Proved with the actual Windows Go/Python processes on 2026-10-21, including all three mapped features. The first passing evidence was `.runs/20261021-193538-1cf0a6/evidence.json`; the boundary/retry update passed in `.runs/20261021-193810-ed4b48/evidence.json`.

Final proof after shutdown/finite-number checks and stricter lost-result assertions is `.runs/20261021-194311-0d2511/evidence.json`. It records one peer transfer for the join, zero extra transfers for the local consumer, worker-loss retry from attempt 1 to 2, and coordinator-restart retry from attempt 1 to 2. All tracked processes exited and the evidence remained after cleanup.

# Doctor

The driver calls authenticated `Status` through the Python gRPC client, waits for both worker IDs and checks all owned process handles. No user cluster is reused.

# Drive

`tools/verify.py` drives `Runtime.put`, `remote(...).remote`, `Runtime.get` and the operational protobuf RPCs. Feature entries under `features/` define assertions. Its fault journeys terminate only an assigned process it started, restart the coordinator against its same bbolt file, and preserve the before/after metadata.

# Evidence

Every run writes `evidence.json` and per-process logs under `.runs/<timestamp>/`. Record actions, returned values, task attempts, object locations, transfer counts, failure messages and cleanup process exit status. This is actual loopback multi-process execution, not a multi-host performance measurement. Keep the tooling in `tools/`; generated data remains ignored.

# Cleanup

The driver's `finally` block terminates and waits only its tracked subprocess handles. On Windows it terminates owned process trees so a task Python child cannot outlive a worker. It retains logs, database and `evidence.json`; verify the evidence file still exists after cleanup. Never kill by executable name.

# Helpers

`tools/verify.py` is the reusable launch, doctor, drive, evidence and cleanup command. Use the feature map to extend an existing journey when behavior changes. Keep the skill current with the maintain-verification-skill workflow.
