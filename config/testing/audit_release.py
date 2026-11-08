"""Inventory and scan the exact Confluent ZIP before publication; uses Syft and Grype."""
import argparse
import hashlib
import io
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import quote
import zipfile

ROOT = Path(__file__).resolve().parents[2]


def run(command, timeout=180):
    subprocess.run([str(part) for part in command], cwd=ROOT, check=True, timeout=timeout)


def archive_contents(archive, version):
    with zipfile.ZipFile(archive) as package:
        manifests = [name for name in package.namelist() if name.endswith('/manifest.json')]
        jars = [name for name in package.namelist() if name.endswith('.jar')]
        if len(manifests) != 1 or len(jars) != 1:
            raise ValueError('Expected one manifest and one shaded JAR in the Confluent ZIP')
        manifest = json.loads(package.read(manifests[0]))
        if (manifest['owner']['username'], manifest['name'], manifest['version']) != ('redis', 'redis-kafka-connect', version):
            raise ValueError('Archive identity or version does not match the requested release')
        return package.read(jars[0])


def complete_inventory(bom, inventory, jar_bytes, version, digest):
    if inventory['version'] != version:
        raise ValueError('Runtime inventory version does not match the release ZIP')
    if inventory['jarSha256'] != hashlib.sha256(jar_bytes).hexdigest():
        raise ValueError('Runtime inventory does not match the ZIP shaded JAR SHA-256')
    components = bom.get('components')
    if not components:
        raise ValueError('Artifact catalog is empty; refusing an incomplete audit')
    purls = {component.get('purl', '').split('?')[0] for component in components}
    runtime_refs = []
    with zipfile.ZipFile(io.BytesIO(jar_bytes)) as shaded:
        bundled = set(shaded.namelist())
        for artifact in inventory['dependencies']:
            for key in ('group', 'name', 'version', 'file'):
                if not artifact.get(key):
                    raise ValueError('Runtime dependency is missing ' + key)
            with zipfile.ZipFile(artifact['file']) as original:
                classes = [name for name in original.namelist() if name.endswith('.class')
                           and not name.startswith('META-INF/') and name != 'module-info.class']
            verified = [name for name in classes if name in bundled
                        or 'com/redis/kafka/connect/shaded/' + name in bundled]
            if not verified:
                raise ValueError('No bundled classes verified for ' + artifact['name'])
            purl = ('pkg:maven/' + quote(artifact['group'], safe='') + '/'
                    + quote(artifact['name'], safe='') + '@' + quote(artifact['version'], safe=''))
            if purl not in purls:
                components.append({'type': 'library', 'group': artifact['group'], 'name': artifact['name'],
                                   'version': artifact['version'], 'purl': purl, 'bom-ref': purl,
                                   'properties': [{'name': 'audit:source', 'value': 'Gradle runtime dependency; bundled classes verified'},
                                                  {'name': 'audit:class', 'value': verified[0]}]})
                purls.add(purl)
            runtime_refs.extend(component['bom-ref'] for component in components
                                if component.get('purl', '').split('?')[0] == purl)
    if not runtime_refs:
        raise ValueError('Runtime dependency inventory is empty')
    root_ref = 'pkg:maven/com.redis/redis-kafka-connect@' + version
    bom.setdefault('metadata', {})['component'] = {
        'type': 'application', 'group': 'com.redis', 'name': 'redis-kafka-connect', 'version': version,
        'bom-ref': root_ref, 'purl': root_ref, 'hashes': [{'alg': 'SHA-256', 'content': digest}]}
    bom.setdefault('dependencies', []).append({'ref': root_ref, 'dependsOn': sorted(set(runtime_refs))})
    return bom


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--version', help='Stable version; defaults to the exact v-prefixed Git release tag')
    parser.add_argument('--inventory', type=Path, help='Prebuilt Gradle runtime inventory (for candidate validation)')
    parser.add_argument('--syft', default='syft')
    parser.add_argument('--grype', default='grype')
    args = parser.parse_args()
    version = args.version
    if version is None:
        tag = subprocess.check_output(['git', 'describe', '--tags', '--exact-match'], cwd=ROOT, text=True).strip()
        if not tag.startswith('v'):
            raise ValueError('Expected a v-prefixed release tag')
        version = tag[1:]
    if not re.fullmatch(r'(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)', version):
        raise ValueError('Expected a stable semantic version without a v prefix')
    prefix = ROOT / 'core/redis-kafka-connect/build/confluent' / ('redis-redis-kafka-connect-' + version)
    archive = Path(str(prefix) + '.zip')
    jar_bytes = archive_contents(archive, version)
    inventory_file = args.inventory
    if inventory_file is None:
        run(['./gradlew', ':redis-kafka-connect:writeRuntimeInventory', '--console=plain'])
        inventory_file = ROOT / 'build/release-audit/runtime-inventory.json'
    inventory = json.loads(inventory_file.read_text())
    report_dir = ROOT / 'build/release-audit'
    report_dir.mkdir(parents=True, exist_ok=True)
    raw_bom = report_dir / ('syft-' + version + '.cdx.json')
    run([args.syft, 'scan', 'file:' + str(archive.relative_to(ROOT)), '--source-name', 'redis-kafka-connect',
         '--source-version', version, '-o', 'cyclonedx-json=' + str(raw_bom)])
    with archive.open('rb') as archive_file:
        digest = hashlib.file_digest(archive_file, 'sha256').hexdigest()
    bom = complete_inventory(json.loads(raw_bom.read_text()), inventory, jar_bytes, version, digest)
    sbom = Path(str(prefix) + '.cdx.json')
    sbom.write_text(json.dumps(bom, indent=2) + '\n')
    report = Path(str(prefix) + '.vulnerabilities.json')
    run([args.grype, 'db', 'update'], timeout=300)
    run([args.grype, 'sbom:' + str(sbom), '--fail-on', 'high', '-o', 'json', '--file', report], timeout=300)
    result = json.loads(report.read_text())
    if result['descriptor']['db']['status']['valid'] is not True:
        raise ValueError('Vulnerability database is not valid')
    matches = result['matches']
    print(f'Release ZIP SHA-256: {digest}')
    print(f'Runtime dependencies verified: {len(inventory["dependencies"])}; known vulnerability matches: {len(matches)}')
    print(f'SBOM: {sbom}\nVulnerability report: {report}')


if __name__ == '__main__':
    main()
