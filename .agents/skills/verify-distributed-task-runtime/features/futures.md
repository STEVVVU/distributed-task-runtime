# Futures and boundaries

## Sub-features
put/get, remote ObjectRef futures, nested dependency DAGs, signature validation, explicit task/dependency failure, authentication.
## How to get to it (user POV)
Construct a Runtime and decorate an installed top-level function with `runtime.remote`.
## Driving it with tools/verify.py
Compare a branching arithmetic DAG against its sequential oracle. Check malformed signatures, unknown references, task failure and denied authentication.
## Gotchas
JSON values only, trusted allowlisted modules, finite numeric values and at most 1 MiB per object. No closures, actors or GPU execution.
