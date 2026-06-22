#!/usr/bin/env bash
# Remove everything the QSR Genie Domains patch created.
# NOTE: deleting the Discover *Domain cards* is UI-only (Discover page -> Manage Domains -> Remove domain).
set -uo pipefail
PROFILE=DEFAULT; WH=d56091a1171f30ff
cd "$(dirname "$0")/.."

echo "== Delete the 4 Discover domains (API) =="
python3 - <<'PY'
import json,subprocess,os
f="genie_domains/domains_created.json"
if os.path.exists(f):
    for tag,did in json.load(open(f)).items():
        p=subprocess.run(["databricks","api","delete",f"/api/2.0/domains/{did}","--profile","DEFAULT"],capture_output=True,text=True)
        print(f"  deleted domain {tag} ({did}) rc={p.returncode}")
else:
    print("  (no domains_created.json)")
PY

echo "== Trash the 4 Genie spaces =="
python3 - <<'PY'
import json,subprocess
s=json.load(open("genie_domains/spaces_created.json"))
for k,v in s.items():
    p=subprocess.run(["databricks","genie","trash-space",v["space_id"],"--profile","DEFAULT"],capture_output=True,text=True)
    print(f"  trashed {v['title']} ({v['space_id']}) rc={p.returncode}")
PY

echo "== Remove tag assignments from the 4 spaces =="
python3 - <<'PY'
import json,subprocess,urllib.parse
s=json.load(open("genie_domains/spaces_created.json"))
for k,v in s.items():
    ep=f"/api/2.0/entity-tag-assignments/geniespaces/{v['space_id']}/tags/{urllib.parse.quote(v['tag'])}"
    subprocess.run(["databricks","api","delete",ep,"--profile","DEFAULT"],capture_output=True,text=True)
    print("  unassigned", v['tag'])
PY

echo "== Drop schema (functions + metric views) and governed tags =="
python3 - <<'PY'
import json,subprocess
WH="d56091a1171f30ff"
def run(stmt):
    p=subprocess.run(["databricks","api","post","/api/2.0/sql/statements","--profile","DEFAULT","--json",
        json.dumps({"warehouse_id":WH,"catalog":"jmrdemo","statement":stmt,"wait_timeout":"40s"})],capture_output=True,text=True)
    try: st=json.loads(p.stdout)["status"]["state"]
    except: st="ERR"
    print(f"  {st}: {stmt[:60]}")
run("DROP SCHEMA IF EXISTS jmrdemo.synth_genie CASCADE")
for t in ["Orders and SOS","Loyalty and Rewards","Inventory and Waste","Workforce and Labor"]:
    run(f"DROP GOVERNED TAG `{t}`")
PY
echo "Done. Remember to delete the 4 Domain cards in the Discover UI."
