# Recovery and publication

## Sub-features
heartbeat loss, bounded retries, durable lineage, root retention, restart fencing and idempotent submissions/publications.
## How to get to it (user POV)
Submit deterministic pure functions and wait on their references.
## Driving it with tools/verify.py
Kill an assigned worker; await reconstruction of pending work and lost outputs. Restart the coordinator with its persisted database. Replay old attempts and identical requests; require stale rejection and duplicate acceptance.
## Gotchas
Retries can repeat side effects. Only pure tasks are supported. A lost coordinator disk is unrecoverable; this is a single durable coordinator, not consensus or HA.
