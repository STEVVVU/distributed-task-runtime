import unittest
from unittest import mock
from pathlib import Path
from types import SimpleNamespace
import json
import tempfile
import benchmark
import benchmark_support
from benchmark import cells,capture_profile,matched_plan
from benchmark_support import load, quantiles,Run


class BenchmarkTests(unittest.TestCase):
    def test_matched_campaign_interleaves_fixed_fixture_and_binary_arms(self):
        plan = list(matched_plan('/candidate/runtime', '/baseline/runtime'))
        self.assertEqual([(item['repeat'], item['arm']) for item in plan], [
            (0, 'candidate'), (0, 'baseline'), (1, 'baseline'), (1, 'candidate'),
            (2, 'candidate'), (2, 'baseline'), (3, 'baseline'), (3, 'candidate'),
            (4, 'candidate'), (4, 'baseline'),
        ])
        self.assertEqual({item['binary'] for item in plan}, {'/candidate/runtime', '/baseline/runtime'})
        self.assertEqual({(item['count'], item['rate']) for item in plan}, {(160, 16)})
        self.assertEqual({tuple(item['cell'].items()) for item in plan}, {
            (('workers', 2), ('object_items', 4096), ('seconds', .02), ('concurrency', 32))
        })

    def test_cells_cover_each_frozen_axis(self):
        values = cells()
        self.assertEqual({v['workers'] for v in values}, {1, 2, 4})
        self.assertEqual({v['concurrency'] for v in values}, {1, 8, 32})
        self.assertEqual(len(values), 9)

    def test_outcomes_and_quantiles(self):
        def action(index):
            if index == 1:
                raise TimeoutError('controlled timeout')
            return {'oracle': index}
        value = load(action, 3, 1)
        self.assertEqual(value['offered'], 3)
        self.assertEqual(value['finished'], 2)
        self.assertEqual(value['timed_out'], 1)
        self.assertEqual(quantiles([1, 3, 2])['p99'], 3)

    def test_native_profile_capture_is_bounded_and_preserves_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'coordinator.pprof'
            response=mock.Mock();response.read.return_value=b'\x1f\x8bsynthetic-profile'
            with mock.patch.object(benchmark.urllib.request,'urlopen') as request:
                request.return_value.__enter__.return_value=response
                proof=capture_profile('127.0.0.1:12345',path)
            self.assertEqual(path.read_bytes(),response.read.return_value)
            self.assertEqual(proof['file'],'coordinator.pprof')
            self.assertEqual(proof['bytes'],len(path.read_bytes()))
            self.assertEqual(proof['requested_seconds'],20)
            request.assert_called_once_with('http://127.0.0.1:12345/debug/pprof/profile?seconds=20',timeout=25)
            response.read.assert_called_once_with((8<<20)+1)

    def test_missing_native_profile_is_not_written(self):
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'worker.pprof'
            with mock.patch.object(benchmark.urllib.request,'urlopen') as request:
                request.return_value.__enter__.return_value.read.return_value=b''
                with self.assertRaisesRegex(AssertionError,'native CPU profile'):capture_profile('127.0.0.1:12345',path)
            self.assertFalse(path.exists())

    def test_profile_reuses_failed_frozen_offered_cell(self):
        def trial(path,cell,count,rate,fault,profile,binary=None):
            self.assertEqual(cell,dict(workers=2,object_items=4096,seconds=.02,concurrency=32))
            self.assertEqual((count,rate,fault,profile),(160,16,None,True))
            return dict(passed=False,native_profiles_passed=True,cleanup=dict(all_exited=True))
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with mock.patch.object(benchmark,'ROOT',root),mock.patch.object(benchmark,'trial',side_effect=trial) as run, \
                 mock.patch.object(benchmark,'environment',return_value={}),mock.patch.object(benchmark.sys,'platform','linux'), \
                 mock.patch.object(benchmark.sys,'argv',['benchmark.py','profile']),mock.patch.object(benchmark.cProfile,'Profile'):
                self.assertEqual(benchmark.main(),1)
                run.assert_called_once()
            summary=json.loads(next((root/'.runs').glob('benchmark-*/summary.json')).read_text())
            self.assertFalse(summary['passed'])
            self.assertTrue(summary['trials'][0]['native_profiles_passed'])

    def test_proc_retains_reaped_child_cpu_and_pid_identity(self):
        fields=['0']*20
        for index,value in {0:'S',1:'1',11:'4',12:'5',13:'6',14:'7',19:'99'}.items():fields[index]=value
        with mock.patch.object(benchmark_support,'os',SimpleNamespace(name='posix')),mock.patch.object(benchmark_support,'Path') as path:
            path.return_value.read_text.side_effect=['10 (worker with spaces) '+' '.join(fields),'VmRSS:\t12 kB\n']
            process=Run.proc(10)
        self.assertEqual((process['cpu_ticks'],process['reaped_child_cpu_ticks'],process['start_time_ticks'],process['parent_pid'],process['rss_bytes']),(9,13,99,1,12*1024))

    def test_descendants_read_all_threads_and_reject_reparented_pid(self):
        def path(value):
            main=mock.Mock();other_thread=mock.Mock();result=mock.Mock()
            main.read_text.return_value=''
            other_thread.read_text.return_value='20 21'
            result.glob.return_value=[main,other_thread] if value=='/proc/10/task' else []
            return result
        def proc(pid):return dict(pid=pid,parent_pid=10 if pid==20 else 999,start_time_ticks=42)
        with mock.patch.object(benchmark_support,'Path',side_effect=path),mock.patch.object(Run,'proc',side_effect=proc):
            self.assertEqual([value['pid'] for value in Run.descendants(10)],[20])


if __name__ == '__main__':
    unittest.main()
