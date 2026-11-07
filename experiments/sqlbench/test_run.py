import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('sqlbench_run', Path(__file__).with_name('run.py'))
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


def sandbox_temp():
    base=(Path.cwd()/'.cache/sqlbench-tests').resolve()
    if not base.is_relative_to(Path.cwd().resolve()):
        raise ValueError('test temporary directory escaped workspace')
    base.mkdir(parents=True,exist_ok=True)
    return tempfile.TemporaryDirectory(dir=base)


class BenchmarkContractTests(unittest.TestCase):
    def test_three_independent_blocks_have_every_factor_once(self):
        jobs=bench.schedule()
        self.assertEqual(len(jobs),16)
        self.assertTrue(all(x['kind']=='pilot' for x in jobs[:4]))
        for n in range(3):
            block=jobs[4+n*4:8+n*4]
            self.assertEqual({x['variant'] for x in block},set(bench.VARIANTS))
            self.assertEqual({x['repeat'] for x in block},{n+1})
        self.assertEqual(jobs,bench.schedule())
        self.assertNotEqual(jobs[4:8],jobs[8:12])

    def test_no_acceptance_from_not_running(self):
        with sandbox_temp() as tmp:
            with patch.object(bench.platform,'system',return_value='Windows'), patch.object(bench.subprocess,'check_output',side_effect=OSError('daemon stopped')):
                result=bench.preflight(Path(tmp))
        self.assertEqual(result['status'],'BLOCKED_RESOURCES')
        self.assertIn('Docker engine/resources unavailable',result['reasons'])

    def test_laptop_resources_rejected_despite_idle_cpu(self):
        reasons=bench.resource_reasons(3737540*1024,23094353920,16,0.05)
        self.assertEqual(len(reasons),2)
        self.assertTrue(any('memory' in r.lower() or 'memavailable' in r.lower() for r in reasons))
        self.assertTrue(any('disk' in r for r in reasons))

    def test_resource_reserve_not_disabled_by_many_cpus(self):
        self.assertTrue(bench.resource_reasons(8*bench.GIB,100*bench.GIB,64,0))
        self.assertTrue(bench.resource_reasons(32*bench.GIB,100*bench.GIB,16,8))
        self.assertFalse(bench.resource_reasons(20*bench.GIB,128*bench.GIB,16,0))

    def test_sha_detects_changed_reference_bytes(self):
        with sandbox_temp() as tmp:
            path=Path(tmp)/'raw.ndjson'
            path.write_bytes(b'{"tick":1}\n')
            old=bench.sha(path)
            path.write_bytes(b'{"tick":2}\n')
            self.assertNotEqual(old,bench.sha(path))

    def test_linux_manifest_relocation_preserves_pin_and_checksums(self):
        source=[dict(groups=16,delta=0,source_commit=bench.PIN,files=[dict(path=r'C:\old\g16-d0\raw.ndjson',sha256='abc')])]
        moved=bench.relocate_manifest(source)
        self.assertEqual(moved[0]['files'][0],dict(path='/golden/g16-d0/raw.ndjson',sha256='abc'))
        self.assertEqual(moved[0]['source_commit'],bench.PIN)
        self.assertEqual(source[0]['files'][0]['path'],r'C:\old\g16-d0\raw.ndjson')

    def test_inspect_credentials_redacted_before_returning_evidence(self):
        raw=[dict(Config=dict(Env=['POSTGRES_PASSWORD=secret-value','PATH=/bin']),HostConfig=dict(Memory=123))]
        with patch.object(bench.subprocess,'check_output',return_value=json.dumps(raw).encode()):
            result=bench.inspect_redacted(['container'],{})
        self.assertNotIn('secret-value',json.dumps(result))
        self.assertEqual(result[0]['HostConfig']['Memory'],123)

    def test_foreign_v1_pin_rejected_before_golden_is_opened(self):
        with sandbox_temp() as tmp:
            root=Path(tmp)
            (root/'configs').mkdir()
            (root/'configs/group-profile.summit-v1.json').write_text(json.dumps({'input_sha256':{}}))
            manifest=root/'manifest.json'
            manifest.write_text(json.dumps([dict(groups=16,delta=0,source_commit='another-commit',files=[])]))
            with patch.object(bench,'sha',return_value=bench.PROFILE):
                with self.assertRaisesRegex(ValueError,'independent golden pin missing'):
                    bench.verify_inputs(root,root,manifest)

    def test_missing_golden_stream_rejected(self):
        with sandbox_temp() as tmp:
            root=Path(tmp)
            (root/'configs').mkdir()
            (root/'configs/group-profile.summit-v1.json').write_text(json.dumps({'input_sha256':{}}))
            manifest=root/'manifest.json'
            manifest.write_text(json.dumps([dict(groups=16,delta=0,source_commit=bench.PIN,files=[])]))
            with patch.object(bench,'sha',return_value=bench.PROFILE):
                with self.assertRaisesRegex(ValueError,'unexpected golden set'):
                    bench.verify_inputs(root,root,manifest)


if __name__=='__main__': unittest.main()
