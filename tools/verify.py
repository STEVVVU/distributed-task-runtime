"""Real cluster driver. All processes, sockets and state are owned by this run."""
import json
import os
from pathlib import Path
import secrets
import socket
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import grpc
from google.protobuf import json_format, struct_pb2
from examples import functions
from taskruntime import ObjectRef, Runtime, TaskError
from taskruntime.client import rpc


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return f"127.0.0.1:{sock.getsockname()[1]}"


def wait(check, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            result = check()
            if result:
                return result
        except grpc.RpcError:
            pass
        time.sleep(0.05)
    raise AssertionError("condition deadline exceeded")


def rejected(action, text="", kind=Exception):
    try:
        action()
    except kind as error:
        assert text in str(error), str(error)
        return str(error)
    raise AssertionError("expected explicit rejection")


def main():
    directory = ROOT / ".runs" / (time.strftime("%Y%m%d-%H%M%S") + "-" + secrets.token_hex(3))
    directory.mkdir(parents=True)
    evidence = {"run": str(directory), "transport": "protobuf Struct over gRPC HTTP/2", "actions": [], "passed": False}
    processes, logs = [], []
    token = secrets.token_urlsafe(32)
    address = port()
    client = Runtime(address, token)
    env = dict(os.environ, TASK_RUNTIME_TOKEN=token, PYTHONPATH=str(ROOT))
    binary = ROOT / "bin" / ("runtime.exe" if os.name == "nt" else "runtime")

    def launch(role, name, endpoint):
        log = open(directory / f"{name}-{len(processes)}.log", "wb")
        logs.append(log)
        command = [str(binary), "-role", role, "-address", endpoint, "-coordinator", address, "-id", name, "-data", str(directory / f"{name}.db"), "-python", sys.executable]
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        processes.append(process)
        evidence["actions"].append({"launch": name, "pid": process.pid, "address": endpoint})
        return process

    def stop(process):
        if process.poll() is None:
            if os.name == "nt":
                subprocess.run(["taskkill", "/PID", str(process.pid), "/T", "/F"], capture_output=True, check=False)
            else:
                process.terminate()
            process.wait(timeout=10)

    def status():
        return client.call({"op": "status"})

    def task(ref):
        return status()["tasks"][ref.id]

    try:
        coordinator = launch("coordinator", "coordinator", address)
        wait(status)
        workers = {name: launch("worker", name, port()) for name in ("node-a", "node-b")}
        wait(lambda: len(status()["nodes"]) == 2)
        assert all(p.poll() is None for p in processes)
        evidence["doctor"] = {"nodes": list(status()["nodes"]), "owned_processes_alive": True}

        large_roots = [client.put([0] * 50000) for _ in range(4)]
        minimum_assignment = struct_pb2.Struct()
        json_format.ParseDict({"inputs": {ref.id: {"root": [0] * 50000, "locations": {}} for ref in large_roots}}, minimum_assignment)
        assignment_bytes = minimum_assignment.ByteSize()
        assert assignment_bytes > 2 << 20
        try:
            oversized = client.remote(functions.slow).remote(large_roots, seconds=0)
        except grpc.RpcError as error:
            assert "assignment exceeds protobuf RPC budget" in str(error), str(error)
            evidence["assignment_size_boundary"] = {"rejected": True, "minimum_protobuf_bytes": assignment_bytes, "error": error.details()}
        else:
            assigned = wait(lambda: task(oversized) if task(oversized)["state"] == "running" else None)
            evidence["assignment_size_boundary"] = {"rejected": False, "minimum_protobuf_bytes": assignment_bytes, "accepted_task": assigned, "node_state": status()["nodes"]}
            raise AssertionError("oversized protobuf assignment was accepted and leased")

        client.call({"op": "put", "id": "x-1", "value": 10})
        collision_task = {"id": "root-x", "module": "examples.functions", "function": "add", "args": [{"kind": "literal", "value": 40}, {"kind": "literal", "value": 59}], "kwargs": {}}
        client.call({"op": "submit", "task": collision_task})
        assert client.get(ObjectRef("root-x")) == 99
        consumer = client.remote(functions.add).remote(ObjectRef("x-1"), 1)
        observed = client.get(consumer)
        evidence["root_cache_namespace"] = {"root_id": "x-1", "producer_id": "root-x", "producer": task(ObjectRef("root-x")), "consumer": task(consumer), "expected": 11, "observed": observed}
        assert observed == 11, f"root/cache namespace collision: expected 11, observed {observed}"

        add, slow, total = [client.remote(f) for f in (functions.add, functions.slow, functions.total)]
        root = client.put(10)
        assert client.get(root) == 10
        assert client.call({"op": "put", "id": root.id, "value": 10})["id"] == root.id
        rejected(lambda: client.call({"op": "put", "id": root.id, "value": 11}), "ID conflict", grpc.RpcError)
        left, right = slow.remote(root), slow.remote(7)
        assert client.get(left) == 10 and client.get(right) == 7
        first = {ref.id: task(ref) for ref in (left, right)}
        assert len({t["worker"] for t in first.values()}) == 2, first
        before_transfers = sum(n["transfers"] for n in status()["nodes"].values())
        joined = total.remote([left, right], offset=5)
        branch = add.remote(left, joined)
        assert client.get(branch) == functions.add(10, functions.total([10, 7], offset=5)) == 32
        after_join = sum(n["transfers"] for n in status()["nodes"].values())
        assert after_join > before_transfers
        local_parent = slow.remote(123, seconds=0.1)
        assert client.get(local_parent) == 123
        local_before = sum(n["transfers"] for n in status()["nodes"].values())
        local_child = add.remote(local_parent, 1)
        assert client.get(local_child) == 124
        local_after = sum(n["transfers"] for n in status()["nodes"].values())
        assert task(local_parent)["worker"] == task(local_child)["worker"]
        assert local_before == local_after
        evidence["dag"] = {"oracle": 32, "result": 32, "initial_tasks": first, "join_transfers": after_join - before_transfers, "local_consumer_transfers": local_after - local_before, "tasks": status()["tasks"]}

        bad = client.remote(functions.fail).remote(1)
        child = add.remote(bad, 1)
        evidence["boundaries"] = {
            "signature": rejected(lambda: add.remote(1), kind=TypeError),
            "unknown_ref": rejected(lambda: add.remote(ObjectRef("missing"), 1), "unknown dependency", grpc.RpcError),
            "auth": rejected(lambda: rpc(address, "wrong", {"op": "status"}), "invalid capability", grpc.RpcError),
            "failure": rejected(lambda: client.get(bad), "synthetic task failure", TaskError),
            "propagation": rejected(lambda: client.get(child), "dependency failed", TaskError),
            "size": rejected(lambda: client.put("x" * ((1 << 20) + 1)), "1 MiB", ValueError),
            "number": rejected(lambda: client.put(float("nan")), "finite", ValueError),
            "module": rejected(lambda: client.call({"op": "submit", "task": {"id": "bad-module", "module": "os", "function": "system", "args": [], "kwargs": {}}}), "allowlisted", grpc.RpcError),
            "raw_number": rejected(lambda: client.call({"op": "put", "id": "bad-number", "value": 1e100}), "exact range", grpc.RpcError),
            "raw_argument": rejected(lambda: client.call({"op": "submit", "task": {"id": "bad-arg", "module": "examples.functions", "function": "add", "args": [{"kind": "oops"}], "kwargs": {}}}), "argument kind", grpc.RpcError),
        }
        original = task(local_child)
        submission = {k: original[k] for k in ("id", "module", "function", "args", "kwargs")}
        assert client.call({"op": "submit", "task": submission})["id"] == local_child.id
        completion = {"op": "complete", "id": local_child.id, "attempt": original["attempt"], "worker": original["worker"]}
        assert client.call(completion)["duplicate"] is True
        rejected(lambda: client.call(dict(completion, attempt=99)), "stale attempt", grpc.RpcError)
        evidence["idempotence"] = {"duplicate_submit": True, "duplicate_completion": True, "stale_rejected": True}

        pending = slow.remote(root, seconds=3)
        assigned = wait(lambda: task(pending) if task(pending)["state"] == "running" else None)
        lost_worker = assigned["worker"]
        lost_output = next(ObjectRef(t["id"]) for t in first.values() if t["worker"] == lost_worker)
        stop(workers[lost_worker])
        evidence["actions"].append({"kill_assigned_worker": lost_worker, "task": assigned})
        assert client.get(pending, timeout=20) == 10
        lost_expected = 10 if lost_output.id == left.id else 7
        assert client.get(lost_output, timeout=20) == lost_expected
        assert task(lost_output)["attempt"] > first[lost_output.id]["attempt"]
        recovered = task(pending)
        assert recovered["attempt"] > assigned["attempt"] and recovered["worker"] != lost_worker
        stale = {"op": "complete", "id": pending.id, "attempt": assigned["attempt"], "worker": lost_worker}
        rejected(lambda: client.call(stale), "stale attempt", grpc.RpcError)
        evidence["worker_loss"] = {"before": assigned, "after": recovered, "reconstructed_output": task(lost_output), "stale_rejected": True}

        assert client.get(branch) == 32

        restart_pending = slow.remote(root, seconds=3)
        restart_assigned = wait(lambda: task(restart_pending) if task(restart_pending)["state"] == "running" else None)
        accepted_before = task(branch)
        evidence["restart_precondition"] = {"accepted_before": accepted_before}
        assert accepted_before["state"] == "done"
        stop(coordinator)
        coordinator = launch("coordinator", "coordinator", address)
        wait(status)
        assert client.get(restart_pending, timeout=20) == 10
        assert client.get(branch) == 32
        assert client.get(root) == 10
        restarted = task(restart_pending)
        assert restarted["attempt"] > restart_assigned["attempt"]
        assert task(branch)["attempt"] == accepted_before["attempt"]
        rejected(lambda: client.call({"op": "complete", "id": restart_pending.id, "worker": restart_assigned["worker"], "attempt": restart_assigned["attempt"]}), "stale attempt", grpc.RpcError)
        evidence["restart"] = {"running_before": restart_assigned, "running_after": restarted, "accepted_result_preserved": True, "root_preserved": True, "precrash_attempt_rejected": True}
        evidence["final_state"] = status()
        evidence["passed"] = True
    except Exception as error:
        evidence["error"] = repr(error)
        raise
    finally:
        for process in reversed(processes):
            stop(process)
        for log in logs:
            log.close()
        evidence["cleanup"] = [{"pid": p.pid, "exit_code": p.poll()} for p in processes]
        path = directory / "evidence.json"
        path.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
        assert path.exists() and all(p.poll() is not None for p in processes)
        print(path)


if __name__ == "__main__":
    main()
