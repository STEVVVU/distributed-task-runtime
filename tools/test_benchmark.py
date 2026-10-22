import unittest
from benchmark import cells
from benchmark_support import load, quantiles


class BenchmarkTests(unittest.TestCase):
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


if __name__ == '__main__':
    unittest.main()
