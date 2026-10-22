import unittest
import time

from taskruntime import ObjectRef, Runtime
from taskruntime.client import encode, rpc, validate


class ClientBoundaries(unittest.TestCase):
    def test_nested_reference_encoding(self):
        self.assertEqual(encode({"values": [ObjectRef("object")]}), {"kind": "dict", "fields": {"values": {"kind": "list", "items": [{"kind": "ref", "ref": "object"}]}}})

    def test_invalid_values_and_depth(self):
        for value in [float("nan"), 2**54, {1: "bad"}, object()]:
            with self.assertRaises((ValueError, TypeError)):
                validate(value)
        value = 1
        for _ in range(34):
            value = [value]
        with self.assertRaises(ValueError):
            encode(value)

    def test_no_closure_or_non_loopback(self):
        def closure():
            return 1
        with self.assertRaises(ValueError):
            Runtime("127.0.0.1:1", "unused").remote(closure)
        with self.assertRaises(ValueError):
            rpc("192.0.2.1:1", "unused", {})

    def test_unreachable_coordinator_obeys_get_deadline(self):
        started = time.monotonic()
        with self.assertRaises(TimeoutError):
            Runtime("127.0.0.1:1", "unused").get(ObjectRef("missing"), timeout=0.15)
        self.assertLess(time.monotonic() - started, 1)


if __name__ == "__main__":
    unittest.main()
