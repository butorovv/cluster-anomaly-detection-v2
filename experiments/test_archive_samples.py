import json
import tempfile
import unittest
from pathlib import Path

import archive_samples as archive


class SampleArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.repo = self.root/'repo'
        self.names = [f'experiments/artifacts/run{i}/metrics-samples.json' for i in range(2)]
        for i, name in enumerate(self.names):
            path = self.repo/name
            path.parent.mkdir(parents=True)
            path.write_bytes(json.dumps([{'CPU': i, 'missing': None}]).encode()+b'\n')

    def create(self):
        return archive.create_archive(self.repo, self.root/'backup', self.names, 'test-pin')

    def test_exact_backup_and_idempotent_restore(self):
        manifest = self.create()
        archive.verify_archive(manifest, self.repo)
        target = self.root/'restored'
        self.assertEqual(archive.restore_archive(manifest, target), 2)
        self.assertEqual(archive.restore_archive(manifest, target), 0)
        for name in self.names:
            self.assertEqual((target/name).read_bytes(), (self.repo/name).read_bytes())

    def test_corruption_rejected_before_restore(self):
        manifest = self.create()
        with (manifest.parent/'metrics-samples.zip').open('ab') as stream:
            stream.write(b'corruption')
        with self.assertRaises(ValueError):
            archive.restore_archive(manifest, self.root/'restore')
        self.assertFalse((self.root/'restore').exists())

    def test_existing_different_target_never_overwritten(self):
        manifest = self.create()
        target = self.root/'restore'
        conflict = target/self.names[1]
        conflict.parent.mkdir(parents=True)
        conflict.write_bytes(b'user data')
        with self.assertRaises(ValueError):
            archive.restore_archive(manifest, target)
        self.assertEqual(conflict.read_bytes(), b'user data')
        self.assertFalse((target/self.names[0]).exists())

    def test_internal_archive_and_unsafe_paths_rejected(self):
        with self.assertRaises(ValueError):
            archive.create_archive(self.repo, self.repo/'backup', self.names, 'pin')
        for name in ('../metrics-samples.json', '/absolute', 'C:\\escape', 'experiments/artifacts/../metrics-samples.json'):
            with self.assertRaises(ValueError):
                archive.member_path(name)


if __name__ == '__main__':
    unittest.main()
