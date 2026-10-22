# Placement and object stores

## Sub-features
locality scoring, worker-local payloads, direct gRPC object transfer, actual worker execution counts.
## How to get to it (user POV)
Submit concurrent independent tasks and consume their futures.
## Driving it with tools/verify.py
Observe tasks completing on two workers, a join requiring transfer and a same-worker consumer requiring none. Save transfer counters and placement metadata.
## Gotchas
Locality minimizes missing dependency objects among idle workers; busy workers are excluded. Counts demonstrate transfers, not a speedup.
