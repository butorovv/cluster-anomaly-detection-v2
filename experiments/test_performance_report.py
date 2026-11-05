import json
import tempfile
import unittest
from pathlib import Path

from performance_report import METRICS, summarize


class PerformanceReportTest(unittest.TestCase):
    def run_file(self, root, name, execution, profile=False, success=True):
        value = {'status': 'PASS' if success else 'FAIL', 'execution_id': execution,
                 'configuration': {'name': name, 'workers': 4, 'mode': 'kafka'},
                 'environment': {'source_commit': 'baseline', 'go': 'go-test',
                                 'worker_gomaxprocs': 1, 'master_gomaxprocs': 2,
                                 'profiling_enabled': profile},
                 'run_id': 'frozen-run', 'config_id': 'frozen-config', 'binary_sha256': {},
                 'comparison': {'status': 'PASS' if success else 'FAIL'},
                 'measurements': {'status': 'PASS', **{key: 1.0 for key in METRICS}}}
        path = root/name
        path.write_text(json.dumps(value), encoding='utf-8')
        return path

    def test_duplicate_execution_is_not_independent_replication(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            a = self.run_file(root, 'a.json', 'same-execution')
            b = self.run_file(root, 'copy.json', 'same-execution')
            with self.assertRaisesRegex(ValueError, 'independent'):
                summarize([a, b])

    def test_profiles_and_failures_never_inflate_clean_sample_size(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = [self.run_file(root, 'a.json', 'a'),
                     self.run_file(root, 'profile.json', 'profile', profile=True),
                     self.run_file(root, 'failed.json', 'failed', success=False)]
            result = summarize(paths)
            self.assertEqual(len(result['excluded']), 1)
            self.assertEqual({g['status'] for g in result['groups']}, {'PROFILE_ONLY', 'PRELIMINARY'})
            self.assertEqual([g['independent_repeats'] for g in result['groups']], [1, 1])


if __name__ == '__main__':
    unittest.main()
