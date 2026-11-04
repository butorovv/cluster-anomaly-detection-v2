import hashlib
import tempfile
import unittest
from pathlib import Path

import archive_results as backup


class ResultsBackupTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        source = self.root/'source'
        source.write_bytes(b'golden\x00\xff\r\n')
        self.entries = [{'path': 'repo/.cache/golden/raw.ndjson',
                         'original_path': str(source), 'bytes': source.stat().st_size,
                         'sha256': hashlib.sha256(source.read_bytes()).hexdigest()}]
        self.manifest = backup.create(self.root/'archive', self.entries, 'test-source')

    def test_exact_restore_and_no_overwrite(self):
        target = self.root/'restored'
        backup.restore(self.manifest, target)
        self.assertEqual((target/self.entries[0]['path']).read_bytes(),
                         Path(self.entries[0]['original_path']).read_bytes())
        with self.assertRaises(FileExistsError):
            backup.restore(self.manifest, target)

    def test_corrupt_archive_never_restored(self):
        archive = self.manifest.parent/'experiment-results.zip'
        archive.write_bytes(archive.read_bytes()+b'corruption')
        with self.assertRaises(ValueError):
            backup.restore(self.manifest, self.root/'restored')
        self.assertFalse((self.root/'restored').exists())

    def test_refuse_existing_archive_and_unsafe_members(self):
        with self.assertRaises(FileExistsError):
            backup.create(self.manifest.parent, self.entries, 'test')
        for name in ('../escape', '/absolute', 'C:\\escape', 'repo/../../escape'):
            with self.assertRaises(ValueError):
                backup.safe_path(name)


if __name__ == '__main__':
    unittest.main()
