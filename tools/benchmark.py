"""Frozen real gRPC task/object benchmark. Run smoke before isolated measurement."""
import argparse
import cProfile
import hashlib
import json
import os
from pathlib import Path
import secrets
import sys
import threading
import time
import urllib.request

from benchmark_support import Run, environment, load, save
from verify import port, wait

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from examples import functions
from taskruntime import Runtime

WORKLOAD = dict(version=1, seed=421, workers=[1, 2, 4], object_items=[16, 4096, 32768], seconds=[0, .02, .2], concurrency=[1, 8, 32], count=32, repeats=5, rates=[1, 4, 16], sweep_seconds=10, stability_seconds=120)
PROFILE_SECONDS=20


def capture_profile(endpoint,path):
    started=time.monotonic()
    started_unix_ns=time.time_ns()
    url=f'http://{endpoint}/debug/pprof/profile?seconds={PROFILE_SECONDS}'
    with urllib.request.urlopen(url,timeout=PROFILE_SECONDS+5) as response:
        data=response.read((8<<20)+1)
    assert 0<len(data)<=8<<20 and data.startswith(b'\x1f\x8b'),'missing, oversized or invalid native CPU profile'
    path.write_bytes(data)
    return dict(file=path.name,bytes=len(data),sha256=hashlib.sha256(data).hexdigest(),request=url,request_started=started,request_started_unix_ns=started_unix_ns,response_finished=time.monotonic(),response_finished_unix_ns=time.time_ns(),requested_seconds=PROFILE_SECONDS)


def cells():
    base = dict(workers=2, object_items=4096, seconds=.02, concurrency=8)
    result = []
    for axis in ['workers', 'object_items', 'seconds', 'concurrency']:
        for value in WORKLOAD[axis]:
            cell = dict(base, **{axis: value})
            if cell not in result:
                result.append(cell)
    return result


def matched_plan(candidate, baseline, repeats=5):
    fixture = dict(workers=2, object_items=4096, seconds=.02, concurrency=32)
    for repeat in range(repeats):
        arms = [('candidate', candidate), ('baseline', baseline)]
        for arm, binary in (arms if repeat % 2 == 0 else reversed(arms)):
            yield dict(repeat=repeat, arm=arm, binary=binary, cell=dict(fixture), count=160, rate=16)


def trial(path, cell, count, rate=None, fault=None, profile=False, binary=None, source=None):
    run = Run(path,capture_descendants=profile)
    result = dict(passed=False, cell=cell, rate=rate, fault=fault)
    token, address = secrets.token_urlsafe(32), port()
    client = Runtime(address, token)
    env = dict(os.environ, TASK_RUNTIME_TOKEN=token, PYTHONPATH=str(ROOT))
    env.pop('TASK_RUNTIME_PROFILE_ADDR',None)
    if profile:
        env['GODEBUG'] = 'schedtrace=1000,gctrace=1'
    name='runtime-profile' if profile else 'runtime'
    binary = Path(binary) if binary else ROOT / 'bin' / (name+'.exe' if os.name == 'nt' else name)
    endpoints={};profile_threads=[];profiles={};profile_errors=[]
    def launch(role, name, endpoint):
        node_env=dict(env)
        if profile:
            endpoints[name]=port()
            node_env['TASK_RUNTIME_PROFILE_ADDR']=endpoints[name]
        return run.launch(name, [str(binary), '-role', role, '-address', endpoint, '-coordinator', address, '-id', name, '-data', str(run.path / (name+'.db')), '-python', sys.executable], node_env)
    def status():
        return client.call({'op': 'status'})
    try:
        result['build'] = dict(binary=str(binary), sha256=hashlib.sha256(binary.read_bytes()).hexdigest(), source=source)
        if profile:result['profile_build']=dict(tag='benchmark',**result['build'])
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
        if profile:
            def capture(name,endpoint):
                try:profiles[name]=capture_profile(endpoint,run.path/(name+'.pprof'))
                except Exception as error:profile_errors.append(dict(name=name,error=repr(error)))
            for name,endpoint in endpoints.items():
                thread=threading.Thread(target=capture,args=(name,endpoint),daemon=True)
                thread.start();profile_threads.append(thread)
        result['load'] = load(action, count, cell['concurrency'], rate)
        result['after'] = status()
        result['transfers'] = sum(n['transfers'] for n in result['after']['nodes'].values()) - sum(n['transfers'] for n in result['before']['nodes'].values())
        result['passed'] = result['load']['finished'] == count
    except Exception as error:
        result['error'] = repr(error)
    finally:
        if profile:
            for thread in profile_threads:thread.join(timeout=PROFILE_SECONDS+10)
            result['native_profiles']=profiles
            result['native_profile_errors']=profile_errors
            result['profile_threads_stopped']=all(not thread.is_alive() for thread in profile_threads)
            result['native_profiles_passed']=set(profiles)==set(endpoints)=={'coordinator','worker-0','worker-1'} and all((run.path/proof['file']).is_file() and (run.path/proof['file']).stat().st_size for proof in profiles.values()) and not profile_errors and result['profile_threads_stopped']
            result['profile_scope']='Benchmark-tag binary only. Parallel native Go CPU profiles requested around the exact failed offered-load cell after warmup, with 20-second bounded windows and receipts. Windows may include idle drain time or end before an unusually long drain; compare native profile timestamps with retained load start/end. Python generator cProfile and descendant/reaped-child resource counters are separate; profile timings are diagnostic only.'
            result['passed']=result['passed'] and result['native_profiles_passed']
        result['cleanup'] = run.cleanup()
        result['passed'] = result['passed'] and result['cleanup']['all_exited'] and not result['cleanup']['errors']
        save(run.path / 'result.json', result)
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['smoke', 'benchmark', 'profile', 'matched'], default='smoke', nargs='?')
    parser.add_argument('--binary')
    parser.add_argument('--candidate-binary')
    parser.add_argument('--baseline-binary')
    parser.add_argument('--candidate-source')
    parser.add_argument('--baseline-source')
    args = parser.parse_args()
    assert args.mode == 'smoke' or sys.platform == 'linux', 'measurement requires isolated Linux runner'
    directory = ROOT / '.runs' / ('benchmark-'+args.mode+'-'+str(time.time_ns()))
    directory.mkdir(parents=True)
    summary = dict(passed=False, mode=args.mode, environment=environment(ROOT), workload=WORKLOAD, trials=[])
    def run(cell, count=WORKLOAD['count'], rate=None, fault=None):
        index = len(summary['trials'])
        value = trial(directory / ('trial-'+str(index)), cell, count, rate, fault, args.mode == 'profile', args.binary)
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
            try:run(dict(workers=2, object_items=4096, seconds=.02, concurrency=32),160,16)
            finally:
                profiler.disable()
                profiler.dump_stats(str(directory / 'generator.pstats'))
            summary['profile_scope'] = 'Native coordinator/two-worker CPU pprof plus generator cProfile and Go scheduler/GC traces on the unchanged failed 16 DAG/s, ten-second cell. Diagnostic timings are excluded from comparisons.'
        elif args.mode == 'matched':
            assert args.candidate_binary and args.baseline_binary and args.candidate_source and args.baseline_source
            assert Path(args.candidate_binary).resolve()!=Path(args.baseline_binary).resolve(),'matched binaries must have distinct paths'
            assert hashlib.sha256(Path(args.candidate_binary).read_bytes()).digest()!=hashlib.sha256(Path(args.baseline_binary).read_bytes()).digest(),'matched binaries must be independently built'
            assert args.baseline_source=='7ed42c3abe0681108ed5ea972138a23db08427d0' and args.candidate_source==summary['environment']['source'] and args.candidate_source!=args.baseline_source,'matched source identity mismatch'
            base = dict(workers=2, object_items=4096, seconds=.02, concurrency=32)
            summary['campaign'] = dict(repeats=5, arms='candidate/baseline interleaved; reversed on odd repeats', count=160, rate=16, seconds=10, fixture=base, candidate_source=args.candidate_source, baseline_source=args.baseline_source)
            for plan in matched_plan(args.candidate_binary, args.baseline_binary):
                repeat, arm = plan['repeat'], plan['arm']
                source = args.candidate_source if arm == 'candidate' else args.baseline_source
                value = trial(directory / ('trial-'+str(len(summary['trials']))), dict(plan['cell'], repeat=repeat, arm=arm), plan['count'], plan['rate'], binary=plan['binary'], source=source)
                summary['trials'].append(value)
                save(directory / 'summary.json', summary)
                print(json.dumps(dict(repeat=repeat, arm=arm, passed=value['passed'], evidence=str(directory))), flush=True)
                if value.get('error') or not value['cleanup']['all_exited'] or not value['cleanup']['sampler_stopped'] or value['cleanup']['errors']:
                    raise RuntimeError(value.get('error', 'cleanup failure'))
            summary['campaign']['completed'] = len(summary['trials']) == 10
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
