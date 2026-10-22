import time


def add(a, b):
    return a + b


def total(values, *, offset=0):
    return sum(values) + offset


def slow(value, seconds=0.7):
    time.sleep(seconds)
    return value


def fail(value):
    raise ValueError("synthetic task failure")
