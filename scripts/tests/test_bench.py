import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import bench
import release


class BenchTests(unittest.TestCase):
    def test_empty_nonfinite_zero_samples_fail(self):
        for samples in ([], [0], [-1], [float('nan')], [float('inf')]):
            with self.subTest(samples=samples), self.assertRaises(ValueError):
                bench.summarize(samples)

    def test_small_sample_does_not_claim_p95(self):
        self.assertIsNone(bench.summarize([1, 2, 3])['p95_ms'])
        self.assertEqual(bench.summarize(list(range(1, 21)))['p95_ms'], 19)

    def test_custom_case_budget_is_honored_and_gate_rejects(self):
        with tempfile.TemporaryDirectory(dir=release.ROOT / 'target') as raw:
            config = Path(raw) / 'budgets.json'
            config.write_text(json.dumps({'case_budgets_p50_ms': {'query-docs': 0.01}}))
            budgets = bench.load_budgets(config)
            rows = [{'id': key, 'samples_ms': [1.0]} for key in budgets]
            results = bench.check_budgets(rows, budgets)
            self.assertFalse(next(row for row in results if row['id'] == 'query-docs')['pass'])
            self.assertEqual(budgets['query-docs'], 0.01)
            with self.assertRaises(ValueError):
                bench.check_budgets([], budgets)
            with self.assertRaises(ValueError):
                bench.check_budgets(rows + [rows[0]], budgets)
            config.write_text('{bad')
            with self.assertRaises(ValueError):
                bench.load_budgets(config)
            config.write_text('{"case_budgets_p50_ms":{"typo":1}}')
            with self.assertRaises(ValueError):
                bench.load_budgets(config)

    def test_base_expected_membership_order_and_values_are_checked(self):
        fixture = {
            'expected_projects': [
                {'path': 'low.md', 'status': 'active', 'priority': 1},
                {'path': 'high.md', 'status': 'active', 'priority': 5},
                {'path': 'hidden.md', 'status': 'archived', 'priority': 9},
            ]
        }
        data = {
            'total': 2,
            'rows': [
                {'file_path': 'high.md', 'values': {'status': 'active', 'priority': 5}},
                {'file_path': 'low.md', 'values': {'status': 'active', 'priority': 1}},
            ],
        }
        bench.validate_project_view({'data': data}, fixture)
        data['rows'].reverse()
        with self.assertRaises(ValueError):
            bench.validate_project_view({'data': data}, fixture)
        data['rows'].reverse()
        data['rows'][0]['values']['priority'] = None
        with self.assertRaises(ValueError):
            bench.validate_project_view({'data': data}, fixture)

    def test_backend_and_cache_labels_are_evidence_checked(self):
        with self.assertRaises(ValueError):
            bench.validate_runtime(
                {'meta': {'runtime': {'backend': 'daemon', 'resultCache': 'hit'}}}, 'direct'
            )
        with self.assertRaises(ValueError):
            bench.validate_runtime(
                {'meta': {'runtime': {'backend': 'daemon', 'resultCache': 'miss'}}}, 'daemon-hit'
            )
        bench.validate_runtime(
            {'meta': {'runtime': {'backend': 'daemon', 'resultCache': 'bypass'}}}, 'daemon-miss'
        )

    def test_versioned_budgets_select_exact_profile_and_mode(self):
        with tempfile.TemporaryDirectory(dir=release.ROOT / 'target') as raw:
            path = Path(raw) / 'budgets.json'
            workload = lambda baseline, budget: {
                'baseline_p50_ms': {key: baseline for key in bench.BUDGET_CASES},
                'case_budgets_p50_ms': {key: budget for key in bench.BUDGET_CASES},
            }
            config = {
                'schema_version': 2,
                'profile': '5k',
                'mode': 'daemon-hit',
                'variant': 'tiny',
                'baseline': {'platform': 'test'},
                'profiles': {
                    '1k': {'direct': workload(10, 20)},
                    '5k': {'daemon-hit': workload(2, 5)},
                },
            }
            path.write_text(json.dumps(config))
            self.assertEqual(
                bench.load_budgets(path, profile='1k', mode='direct')['query-docs'], 20
            )
            self.assertEqual(bench.load_budgets(path)['query-docs'], 5)
            with self.assertRaises(ValueError):
                bench.load_budgets(path, profile='1k', mode='daemon-hit')
            with self.assertRaises(ValueError):
                bench.load_budgets(path, variant='representative')
            del config['profiles']['1k']['direct']['baseline_p50_ms']['query-docs']
            path.write_text(json.dumps(config))
            with self.assertRaises(ValueError):
                bench.load_budgets(path)

    def test_budget_fixture_identity_cannot_drift_from_baseline(self):
        configured = {
            'schema_version': 2,
            'baseline': {'fixture_seed': 42, 'fixture_sha256': {'1k': 'expected'}},
        }
        bench.validate_budget_fixture(
            configured, {'profile': '1k', 'seed': 42, 'sha256': 'expected'}
        )
        for fixture in (
            {'profile': '1k', 'seed': 43, 'sha256': 'expected'},
            {'profile': '1k', 'seed': 42, 'sha256': 'changed'},
        ):
            with self.assertRaises(ValueError):
                bench.validate_budget_fixture(configured, fixture)

    def test_invalid_result_cannot_be_a_fast_success(self):
        with self.assertRaises(ValueError):
            bench.envelope('{"ok":false,"data":{}}')
        with self.assertRaises(ValueError):
            bench.validate_answer('query-docs', {'data': {'items': []}}, {'seed': 42})
        with self.assertRaises(ValueError):
            bench.validate_answer('search-no-result', {'data': {'path': 'unexpected.md'}}, {})

    def test_paths_and_filters_remain_argument_vectors(self):
        argv = bench.command(
            Path('/tmp/space dir/tao'),
            ['search', 'a; $(touch bad)'],
            Path('/tmp/my vault'),
            Path('/tmp/db file'),
            'direct',
            Path('/tmp/socket'),
        )
        self.assertIn('/tmp/my vault', argv)
        self.assertIn('a; $(touch bad)', argv)
        self.assertEqual(argv[1:3], ['--execution-mode', 'direct'])

    def test_benchmark_environment_isolates_ambient_application_state(self):
        with patch.dict(
            'os.environ',
            {
                'TAO_DB_PATH': '/unrelated/index.sqlite',
                'TAO_DATA_DIR': '/unrelated/state',
                'TAO_READ_ONLY': 'false',
            },
        ):
            env = bench.benchmark_environment(Path('/repository/report'), '/repository/tmp')
        self.assertNotIn('TAO_DB_PATH', env)
        self.assertEqual(env['TAO_DATA_DIR'], '/repository/report/runtime-state')
        self.assertNotIn('TAO_READ_ONLY', env)
