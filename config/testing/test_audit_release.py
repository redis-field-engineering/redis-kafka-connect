import hashlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import zipfile

from audit_release import archive_contents, complete_inventory, main


class ReleaseAuditTest(unittest.TestCase):
    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory()
        self.addCleanup(self.scratch.cleanup)
        self.original = Path(self.scratch.name) / 'reactive.jar'
        with zipfile.ZipFile(self.original, 'w') as jar:
            jar.writestr('example/Publisher.class', b'class bytes')
        self.inventory = {'version': '1.2.0', 'dependencies': [
            {'group': 'example', 'name': 'reactive', 'version': '1.0.0', 'file': str(self.original)}]}
        self.jar = io.BytesIO()
        with zipfile.ZipFile(self.jar, 'w') as jar:
            jar.writestr('com/redis/kafka/connect/shaded/example/Publisher.class', b'class bytes')
        self.inventory['jarSha256'] = hashlib.sha256(self.jar.getvalue()).hexdigest()
        self.bom = {'components': [{'type': 'application', 'name': 'connector', 'bom-ref': 'connector'}]}

    def test_shading_cannot_hide_a_runtime_dependency_from_the_sbom(self):
        bom = complete_inventory(self.bom, self.inventory, self.jar.getvalue(), '1.2.0', '0' * 64)
        libraries = [c for c in bom['components'] if c['type'] == 'library']
        self.assertEqual(['pkg:maven/example/reactive@1.0.0'], [c['purl'] for c in libraries])
        self.assertIn(libraries[0]['bom-ref'], bom['dependencies'][-1]['dependsOn'])

    def test_unverified_dependency_prevents_a_clear_scan(self):
        empty = io.BytesIO()
        with zipfile.ZipFile(empty, 'w'):
            pass
        self.inventory['jarSha256'] = hashlib.sha256(empty.getvalue()).hexdigest()
        with self.assertRaisesRegex(ValueError, 'No bundled classes verified'):
            complete_inventory(self.bom, self.inventory, empty.getvalue(), '1.2.0', '0' * 64)

    def test_same_version_and_class_paths_cannot_hide_a_different_shaded_jar(self):
        stale = io.BytesIO()
        with zipfile.ZipFile(stale, 'w') as jar:
            jar.writestr('com/redis/kafka/connect/shaded/example/Publisher.class', b'old vulnerable bytes')
        with self.assertRaisesRegex(ValueError, 'SHA-256'):
            complete_inventory(self.bom, self.inventory, stale.getvalue(), '1.2.0', '0' * 64)

    def test_stale_runtime_inventory_is_rejected(self):
        self.inventory['version'] = '1.1.0'
        with self.assertRaisesRegex(ValueError, 'version does not match'):
            complete_inventory(self.bom, self.inventory, self.jar.getvalue(), '1.2.0', '0' * 64)

    def test_empty_artifact_catalog_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'catalog is empty'):
            complete_inventory({}, self.inventory, self.jar.getvalue(), '1.2.0', '0' * 64)

    def test_existing_qualified_purl_does_not_duplicate_a_library(self):
        self.bom['components'].append({'type': 'library', 'name': 'reactive', 'bom-ref': 'existing',
                                       'purl': 'pkg:maven/example/reactive@1.0.0?type=jar'})
        bom = complete_inventory(self.bom, self.inventory, self.jar.getvalue(), '1.2.0', '0' * 64)
        self.assertEqual(1, len([c for c in bom['components'] if c['type'] == 'library']))
        self.assertIn('existing', bom['dependencies'][-1]['dependsOn'])

    def test_wrong_archive_version_is_rejected(self):
        archive = Path(self.scratch.name) / 'candidate.zip'
        with zipfile.ZipFile(archive, 'w') as package:
            package.writestr('connector/manifest.json', json.dumps(
                {'owner': {'username': 'redis'}, 'name': 'redis-kafka-connect', 'version': '1.1.0'}))
            package.writestr('connector/lib/connector.jar', self.jar.getvalue())
        with self.assertRaisesRegex(ValueError, 'identity or version'):
            archive_contents(archive, '1.2.0')

    def test_snapshot_version_cannot_be_a_stable_release(self):
        with patch('sys.argv', ['audit_release.py', '--version', '1.2.0-SNAPSHOT']):
            with self.assertRaisesRegex(ValueError, 'stable semantic version'):
                main()


if __name__ == '__main__':
    unittest.main()
