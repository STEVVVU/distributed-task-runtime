"""One invocation per task; never imports modules outside the operator allowlist."""
import contextlib
import importlib
import inspect
import json
import sys

from .client import MAX_OBJECT, validate


def main():
    try:
        request = json.loads(sys.stdin.buffer.read(2 << 20))
        if request["module"] not in request["modules"] or not request["function"].isidentifier() or request["function"].startswith("_"):
            raise ValueError("function is not allowlisted")
        function = getattr(importlib.import_module(request["module"]), request["function"])
        if not inspect.isfunction(function) or function.__module__ != request["module"] or function.__qualname__ != function.__name__:
            raise ValueError("only this module's top-level functions may execute")
        inspect.signature(function).bind(*request["args"], **request["kwargs"])
        # Task stdout must not corrupt the machine-readable result channel.
        with contextlib.redirect_stdout(sys.stderr):
            result = function(*request["args"], **request["kwargs"])
        validate(result)
        output = json.dumps({"value": result}, allow_nan=False)
        if len(output.encode()) > MAX_OBJECT:
            raise ValueError("result exceeds 1 MiB")
    except Exception as error:
        output = json.dumps({"error": f"{type(error).__name__}: {error}"[:2000]})
    print(output)


if __name__ == "__main__":
    main()
