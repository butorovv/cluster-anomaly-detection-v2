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
            clean = next(g for g in result['groups'] if not g['profile'])
            self.assertEqual(clean['independent_repeats'], 1)
            self.assertEqual((clean['attempt_count'], clean['pass_count'], clean['fail_count']), (2, 1, 1))
            self.assertEqual({a['golden_verdict'] for a in clean['attempts']}, {'PASS', 'FAIL'})

    def test_failure_before_comparator_is_not_golden_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.run_file(Path(directory), 'failed.json', 'failed', success=False)
            run = json.loads(path.read_text(encoding='utf-8'))
            run['comparison']['status'] = ''
            path.write_text(json.dumps(run), encoding='utf-8')
            group = summarize([path])['groups'][0]
            self.assertEqual(group['attempts'][0]['golden_verdict'], 'NOT_REACHED')
            self.assertEqual(group['status'], 'NO_SUCCESSFUL_MEASUREMENT')
            self.assertEqual(group['metrics'], {})

    def test_distinct_os_and_resource_contexts_never_merge(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = []
            for name, system, resource in [('windows', 'windows', 'native'),
                                            ('linux-a', 'linux', 'quota-a'),
                                            ('linux-b', 'linux', 'quota-b')]:
                path = self.run_file(root, name+'.json', name)
                run = json.loads(path.read_text(encoding='utf-8'))
                run['environment'].update(os=system, resource_context_sha256=resource)
                path.write_text(json.dumps(run), encoding='utf-8')
                paths.append(path)
            result = summarize(paths)
            self.assertEqual(len(result['groups']), 3)
            self.assertTrue(all(group['independent_repeats'] == 1 for group in result['groups']))


if __name__ == '__main__':
    unittest.main()
