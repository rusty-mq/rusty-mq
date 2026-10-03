#!/usr/bin/env sh
# Generate a minimal dependency SBOM (crate, version, licenses) from cargo
# metadata. No secrets, no payloads. Output: SBOM.csv on stdout.
set -eu
cargo metadata --format-version 1 --no-deps >/dev/null 2>&1 || {
  echo "run from a cargo workspace root" >&2; exit 1; }
cargo metadata --format-version 1 |
  python3 -c '
import json, sys
meta = json.load(sys.stdin)
print("crate,version,licenses,manifest_path")
for pkg in sorted(meta["packages"], key=lambda p: p["name"]):
    if pkg["source"] is None:  # workspace members
        continue
    name = pkg["name"]; ver = pkg["version"]
    lic = pkg.get("license", ""); path = pkg["manifest_path"]
    print(f"{name},{ver},{lic},{path}")
'
