#!/usr/bin/env bash
# Pin scanner versions and verify upstream checksums before executing them.
set -euo pipefail
cd "$(dirname "$0")/../.."
case "$(uname -s)" in
  Linux) audit_os=linux ;;
  Darwin) audit_os=darwin ;;
  *) echo 'Unsupported audit operating system' >&2; exit 1 ;;
esac
case "$(uname -m)" in
  x86_64) audit_arch=amd64 ;;
  arm64|aarch64) audit_arch=arm64 ;;
  *) echo 'Unsupported audit architecture' >&2; exit 1 ;;
esac
for spec in syft:1.57.0 grype:0.121.2; do
  tool=${spec%:*}
  version=${spec#*:}
  destination="build/release-audit/tools/$tool-$version"
  mkdir -p "$destination"
  if [ ! -x "$destination/$tool" ]; then
    archive="${tool}_${version}_${audit_os}_${audit_arch}.tar.gz"
    checksums="${tool}_${version}_checksums.txt"
    base="https://github.com/anchore/$tool/releases/download/v$version"
    curl --fail --silent --show-error --location --connect-timeout 15 --max-time 180 "$base/$archive" -o "$destination/$archive"
    curl --fail --silent --show-error --location --connect-timeout 15 --max-time 60 "$base/$checksums" -o "$destination/$checksums"
    python3 - "$destination/$archive" "$destination/$checksums" <<'PY'
import hashlib
from pathlib import Path
import sys
archive, checksums = map(Path, sys.argv[1:])
expected = [line.split()[0] for line in checksums.read_text().splitlines()
            if line.split()[-1].lstrip('*') == archive.name]
if len(expected) != 1 or hashlib.sha256(archive.read_bytes()).hexdigest() != expected[0]:
    raise SystemExit('Scanner checksum verification failed: ' + archive.name)
PY
    tar -xzf "$destination/$archive" -C "$destination" "$tool"
  fi
done
python3 config/testing/audit_release.py \
  --syft "$PWD/build/release-audit/tools/syft-1.57.0/syft" \
  --grype "$PWD/build/release-audit/tools/grype-0.121.2/grype" "$@"
