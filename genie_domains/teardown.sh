#!/usr/bin/env bash
# Tear down the QSR Genie layer: BU domains + governed tags, the 11 spaces, and synth_genie.
# Thin wrapper over the shared modules (same logic the destroy-job notebook runs headless).
set -uo pipefail
cd "$(dirname "$0")/.."

echo "== Delete BU domains + governed tags =="
python3 genie_domains/build_domains.py teardown

echo "== Trash the 11 Genie spaces =="
python3 - <<'PY'
import json, subprocess, os
f = "genie_domains/spaces_created.json"
if os.path.exists(f):
    for k, v in json.load(open(f)).items():
        p = subprocess.run(["databricks", "genie", "trash-space", v["space_id"], "--profile", "DEFAULT"],
                           capture_output=True, text=True)
        print(f"  trashed {v['title']} rc={p.returncode}")
else:
    print("  (no spaces_created.json)")
PY

echo "== Drop synth_genie schema (functions + metric views) =="
python3 - <<'PY'
import json, subprocess
body = json.dumps({"warehouse_id": "d56091a1171f30ff", "catalog": "jmrdemo",
                   "statement": "DROP SCHEMA IF EXISTS jmrdemo.synth_genie CASCADE", "wait_timeout": "40s"})
p = subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "--profile", "DEFAULT", "--json", body],
                   capture_output=True, text=True)
try:    print("  DROP synth_genie:", json.loads(p.stdout)["status"]["state"])
except Exception:  print("  DROP synth_genie: ERR", p.stdout[:120], p.stderr[:120])
PY
echo "Done."
