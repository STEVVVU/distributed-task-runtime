"""Trusted, loopback-only distributed futures using real protobuf gRPC."""
import inspect
import ipaddress
import json
import math
import time
import uuid
from dataclasses import dataclass

import grpc
from google.protobuf import json_format, struct_pb2

MAX_OBJECT = 1 << 20


def validate(value, depth=0):
    if depth > 32:
        raise ValueError("value nesting exceeds 32")
    if value is None or isinstance(value, (str, bool)):
        return
    if isinstance(value, (int, float)):
        if not math.isfinite(value) or abs(value) > 2**53 - 1:
            raise ValueError("numbers must be finite and within exact protobuf Struct integer range")
        return
    if isinstance(value, (list, tuple)):
        for child in value:
            validate(child, depth + 1)
        return
    if isinstance(value, dict) and all(isinstance(k, str) for k in value):
        for child in value.values():
            validate(child, depth + 1)
        return
    raise TypeError("only JSON values and string dictionary keys are supported")


def rpc(address, token, request, timeout=3):
    host, _ = address.rsplit(":", 1)
    if not ipaddress.ip_address(host.strip("[]")).is_loopback:
        raise ValueError("non-loopback peer rejected")
    message = struct_pb2.Struct()
    json_format.ParseDict(request, message)
    with grpc.insecure_channel(address, options=[("grpc.max_receive_message_length", 2 << 20), ("grpc.max_send_message_length", 2 << 20)]) as channel:
        call = channel.unary_unary("/runtime.Runtime/Call", request_serializer=struct_pb2.Struct.SerializeToString, response_deserializer=struct_pb2.Struct.FromString)
        result = call(message, timeout=timeout, metadata=[("authorization", token)])
    return json_format.MessageToDict(result)


@dataclass(frozen=True)
class ObjectRef:
    id: str


class TaskError(RuntimeError):
    pass


def encode(value, depth=0):
    if depth > 32:
        raise ValueError("argument nesting exceeds 32")
    if isinstance(value, ObjectRef):
        return {"kind": "ref", "ref": value.id}
    if isinstance(value, (list, tuple)):
        return {"kind": "list", "items": [encode(v, depth + 1) for v in value]}
    if isinstance(value, dict):
        if not all(isinstance(k, str) for k in value):
            raise TypeError("dictionary keys must be strings")
        return {"kind": "dict", "fields": {k: encode(v, depth + 1) for k, v in value.items()}}
    validate(value)
    return {"kind": "literal", "value": value}


class Remote:
    def __init__(self, runtime, function):
        if function.__module__ == "__main__" or "<locals>" in function.__qualname__ or function.__qualname__ != function.__name__:
            raise ValueError("remote functions must be installed top-level module functions")
        self.runtime, self.function = runtime, function

    def remote(self, *args, **kwargs):
        inspect.signature(self.function).bind(*args, **kwargs)
        task = {"id": uuid.uuid4().hex, "module": self.function.__module__, "function": self.function.__name__, "args": [encode(a) for a in args], "kwargs": {k: encode(v) for k, v in kwargs.items()}}
        self.runtime.call({"op": "submit", "task": task})
        return ObjectRef(task["id"])


class Runtime:
    def __init__(self, address, token):
        self.address, self.token = address, token

    def call(self, request, timeout=3):
        return rpc(self.address, self.token, request, timeout)

    def remote(self, function):
        return Remote(self, function)

    def put(self, value):
        validate(value)
        if len(json.dumps(value).encode()) > MAX_OBJECT:
            raise ValueError("object exceeds 1 MiB")
        identifier = uuid.uuid4().hex
        self.call({"op": "put", "id": identifier, "value": value})
        return ObjectRef(identifier)

    def get(self, ref, timeout=20):
        if not isinstance(ref, ObjectRef):
            raise TypeError("get requires an ObjectRef")
        deadline = time.monotonic() + timeout
        last_error = "object unavailable"
        while time.monotonic() < deadline:
            try:
                response = self.call({"op": "locate", "id": ref.id}, timeout=min(3, max(0.001, deadline - time.monotonic())))
                if response["state"] == "failed":
                    raise TaskError(response["error"])
                obj = response["object"]
                if "root" in obj:
                    return obj["root"]
                if response["state"] == "done":
                    for location in obj["locations"].values():
                        try:
                            return rpc(location["address"], self.token, {"op": "fetch", "key": location["key"]}, timeout=min(3, max(0.001, deadline - time.monotonic())))["value"]
                        except grpc.RpcError as error:
                            last_error = error.details()
            except grpc.RpcError as error:
                if error.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                    raise
                last_error = error.details()
            time.sleep(0.05)
        raise TimeoutError(f"get deadline exceeded: {last_error}")
