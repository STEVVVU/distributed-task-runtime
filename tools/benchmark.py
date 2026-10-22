"""Frozen real gRPC task/object benchmark. Run smoke before isolated measurement."""
import argparse
import cProfile
import hashlib
import json
import os
from pathlib import Path
import secrets
import sys
import time

from benchmark_support import Run, environment, load, save
from verify import port, wait

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from examples import functions
from taskruntime import Runtime

WORKLOAD = dict(version=1, seed=421, workers=[1, 2, 4], object_items=[16, 4096, 32768], seconds=[0, .02, .2], concurrency=[1, 8, 32], count=32, repeats=5, rates=[1, 4, 16], sweep_seconds=10, stability_seconds=120)


def cells():
    base = dict(workers=2, object_items=4096, seconds=.02, concurrency=8)
    result = []
    for axis in ['workers', 'object_items', 'seconds', 'concurrency']:
        for value in WORKLOAD[axis]:
            cell = dict(base, **{axis: value})
            if cell not in result:
                result.append(cell)
    return result


def trial(path, cell, count, rate=None, fault=None, profile=False):
    run = Run(path)
    result = dict(passed=False, cell=cell, rate=rate, fault=fault)
    token, address = secrets.token_urlsafe(32), port()
    client = Runtime(address, token)
    env = dict(os.environ, TASK_RUNTIME_TOKEN=token, PYTHONPATH=str(ROOT))
    if profile:
        env['GODEBUG'] = 'schedtrace=1000,gctrace=1'
    binary = ROOT / 'bin' / ('runtime.exe' if os.name == 'nt' else 'runtime')
    def launch(role, name, endpoint):
        return run.launch(name, [str(binary), '-role', role, '-address', endpoint, '-coordinator', address, '-id', name, '-data', str(run.path / (name+'.db')), '-python', sys.executable], env)
    def status():
        return client.call({'op': 'status'})
    try:
        coordinator = launch('coordinator', 'coordinator', address)
        wait(status)
        workers = {f'worker-{i}': launch('worker', f'worker-{i}', port()) for i in range(cell['workers'])}
        wait(lambda: len(status()['nodes']) == cell['workers'])
        values = [(i * 17 + WORKLOAD['seed']) % 97 for i in range(cell['object_items'])]
        root = client.put(values)
        result.update(input_bytes=len(json.dumps(values).encode()), input_sha256=hashlib.sha256(json.dumps(values).encode()).hexdigest())
        slow, total = client.remote(functions.slow), client.remote(functions.total)
        def action(index):
            submitted = time.monotonic()
            parent = slow.remote(root, seconds=3 if fault else cell['seconds'])
            child = total.remote(parent, offset=index)
            fault_proof = None
            if fault:
                assigned = wait(lambda: status()['tasks'][parent.id] if status()['tasks'][parent.id]['state'] == 'running' else None)
                fault_at = time.monotonic()
                if fault == 'worker-loss':
                    run.stop(workers[assigned['worker']])
                else:
                    run.stop(coordinator)
                    launch('coordinator', 'coordinator', address)
                    wait(status)
                fault_proof = dict(at=fault_at, before=assigned)
            actual = client.get(child, timeout=40)
            assert actual == sum(values)+index, (actual, sum(values)+index)
            state = status()
            tasks = {ref.id: state['tasks'][ref.id] for ref in [parent, child]}
            if fault:
                assert tasks[parent.id]['attempt'] > assigned['attempt'], tasks
                fault_proof.update(after=tasks[parent.id], recovery_ms=1000*(time.monotonic()-fault_at))
            return dict(result=actual, expected=sum(values)+index, parent=parent.id, child=child.id, tasks=tasks, submitted=submitted, fault=fault_proof)
        if not fault:
            result['warmup'] = action(-1)
        result['before'] = status()
        result['load'] = load(action, count, cell['concurrency'], rate)
        result['after'] = status()
        result['transfers'] = sum(n['transfers'] for n in result['after']['nodes'].values()) - sum(n['transfers'] for n in result['before']['nodes'].values())
        result['passed'] = result['load']['finished'] == count
    except Exception as error:
        result['error'] = repr(error)
    finally:
        result['cleanup'] = run.cleanup()
        result['passed'] = result['passed'] and result['cleanup']['all_exited'] and not result['cleanup']['errors']
        save(run.path / 'result.json', result)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['smoke', 'benchmark', 'profile'], default='smoke', nargs='?')
    args = parser.parse_args()
    assert args.mode == 'smoke' or sys.platform == 'linux', 'measurement requires isolated Linux runner'
    directory = ROOT / '.runs' / ('benchmark-'+args.mode+'-'+str(time.time_ns()))
    directory.mkdir(parents=True)
    summary = dict(passed=False, mode=args.mode, environment=environment(ROOT), workload=WORKLOAD, trials=[])
    def run(cell, count=WORKLOAD['count'], rate=None, fault=None):
        index = len(summary['trials'])
        value = trial(directory / ('trial-'+str(index)), cell, count, rate, fault, args.mode == 'profile')
        summary['trials'].append(value)
        save(directory / 'summary.json', summary)
        print(json.dumps(dict(trial=index, passed=value['passed'], evidence=str(directory))), flush=True)
        if value.get('error') or not value['cleanup']['all_exited']:
            raise RuntimeError(value.get('error', 'cleanup failure'))
        return value
    try:
        if args.mode == 'smoke':
            run(dict(workers=2, object_items=16, seconds=0, concurrency=1), 1)
        elif args.mode == 'profile':
            profiler = cProfile.Profile()
            profiler.enable()
            run(dict(workers=2, object_items=4096, seconds=.02, concurrency=8))
            profiler.disable()
            profiler.dump_stats(str(directory / 'generator.pstats'))
            summary['profile_scope'] = 'Generator cProfile and Go scheduler/GC traces. No runtime CPU-profile endpoint exists; service hot-path CPU profiling remains a prerequisite before cause-dependent fixes.'
        else:
            matrix = cells()
            for repeat in range(WORKLOAD['repeats']):
                for cell in matrix if repeat % 2 == 0 else reversed(matrix):
                    run(dict(cell, repeat=repeat))
            base = dict(workers=2, object_items=4096, seconds=.02, concurrency=32)
            sweeps = [run(base, int(rate*WORKLOAD['sweep_seconds']), rate) for rate in WORKLOAD['rates']]
            eligible = [v for v in sweeps if v['passed'] and v['load']['dispatch_lag_ms']['p99'] < 100 and v['load']['latency_ms']['p99'] < 5000]
            if eligible:
                rate = eligible[-1]['rate']
                run(base, int(rate*WORKLOAD['stability_seconds']), rate)
            else:
                summary['stability_blocked'] = 'No zero-loss rate with dispatch p99<100ms and end-to-end p99<5s; expand lower rates before claiming stability.'
            for fault in ['worker-loss', 'coordinator-restart']:
                run(dict(workers=2, object_items=16, seconds=0, concurrency=1), 1, fault=fault)
        summary['passed'] = all(v['passed'] for v in summary['trials']) and not summary.get('stability_blocked')
    except Exception as error:
        summary['error'] = repr(error)
    finally:
        save(directory / 'summary.json', summary)
    print(directory / 'summary.json')
    return 0 if summary['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
