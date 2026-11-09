import hashlib
import unittest

from prepare_github_release import DEPLOYMENT, recovery_config


class GithubRecoveryTest(unittest.TestCase):
    def setUp(self):
        self.jar = b'published runtime JAR'
        self.digest = hashlib.sha256(self.jar).hexdigest()
        self.inventory = {'version': '1.2.0', 'jarSha256': self.digest}

    def test_matching_jar_disables_only_maven(self):
        config = 'signing:\n  active: ALWAYS\n' + DEPLOYMENT + '        authorization: BASIC\n'
        self.assertEqual(recovery_config(config, self.inventory, self.digest + '\n', self.jar),
                         config.replace('active: RELEASE', 'active: NEVER'))

    def test_unpublished_or_different_jar_is_rejected(self):
        for digest in ('', '<html>404</html>', 'f' * 64):
            with self.subTest(digest=digest), self.assertRaises(ValueError):
                recovery_config(DEPLOYMENT, self.inventory, digest, self.jar)

    def test_stale_inventory_cannot_approve_different_bytes(self):
        with self.assertRaises(ValueError):
            recovery_config(DEPLOYMENT, self.inventory, self.digest, b'different JAR')

    def test_snapshot_inventory_is_rejected(self):
        self.inventory['version'] = '1.2.0-SNAPSHOT'
        with self.assertRaises(ValueError):
            recovery_config(DEPLOYMENT, self.inventory, self.digest, self.jar)

    def test_unexpected_deployment_configuration_is_rejected(self):
        for config in ('', DEPLOYMENT * 2):
            with self.subTest(config=config), self.assertRaises(ValueError):
                recovery_config(config, self.inventory, self.digest, self.jar)


if __name__ == '__main__':
    unittest.main()
