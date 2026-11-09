"""Disable Maven deployment only after verifying the already-published runtime JAR."""
import hashlib
import json
from pathlib import Path
import re
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
DEPLOYMENT = 'deploy:\n  maven:\n    mavenCentral:\n      redis:\n        active: RELEASE\n'


def recovery_config(config, inventory, published_digest, jar_bytes):
    version = inventory['version']
    if not re.fullmatch(r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', version):
        raise ValueError('Expected a stable release inventory')
    digest = published_digest.strip()
    if not re.fullmatch(r'[0-9a-f]{64}', digest):
        raise ValueError('Invalid Maven Central JAR checksum')
    if digest != inventory['jarSha256'] or digest != hashlib.sha256(jar_bytes).hexdigest():
        raise ValueError('Local JAR differs from the already-published Maven Central JAR')
    if config.count(DEPLOYMENT) != 1:
        raise ValueError('Expected exactly one configured Maven deployment')
    return config.replace(DEPLOYMENT, DEPLOYMENT.replace('active: RELEASE', 'active: NEVER'), 1)


def main():
    inventory = json.loads((ROOT / 'build/release-audit/runtime-inventory.json').read_text())
    version = inventory['version']
    if not re.fullmatch(r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', version):
        raise ValueError('Expected a stable release inventory')
    filename = f'redis-kafka-connect-{version}-all.jar'
    url = f'https://repo.maven.apache.org/maven2/com/redis/redis-kafka-connect/{version}/{filename}.sha256'
    with urllib.request.urlopen(url, timeout=30) as response:
        published_digest = response.read(4096).decode('ascii')
    jar_bytes = (ROOT / 'core/redis-kafka-connect/build/libs' / filename).read_bytes()
    config = ROOT / 'jreleaser.yml'
    config.write_text(recovery_config(config.read_text(), inventory, published_digest, jar_bytes))
    print('Published Maven JAR matches; Maven deployment disabled for this GitHub recovery run')


if __name__ == '__main__':
    main()
