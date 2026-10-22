# Futures and boundaries

## Sub-features
put/get, remote ObjectRef futures, nested dependency DAGs, signature validation, explicit task/dependency failure, authentication, root/task object-key isolation and protobuf dispatch budgets.
## How to get to it (user POV)
Construct a Runtime and decorate an installed top-level function with `runtime.remote`.
## Driving it with tools/verify.py
Compare a branching arithmetic DAG against its sequential oracle. Check malformed signatures, unknown references, task failure and denied authentication. Complete task `root-x=99`, consume root `x-1=10` and require `add(root,1)=11`. Four roots of 50,000 zeros each must be rejected as an oversized protobuf assignment before a task is accepted.
## Gotchas
JSON values only, trusted allowlisted modules, finite numeric values and at most 1 MiB per object. No closures, actors or GPU execution.
