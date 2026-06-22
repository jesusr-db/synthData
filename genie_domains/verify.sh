#!/usr/bin/env bash
# Health check for the QSR Genie Domains patch.
set -uo pipefail
PROFILE=DEFAULT; WH=d56091a1171f30ff
cd "$(dirname "$0")/.."

echo "== Genie spaces =="
python3 - <<'PY'
import json,subprocess
s=json.load(open("genie_domains/spaces_created.json"))
for k,v in s.items():
    p=subprocess.run(["databricks","genie","get-space",v["space_id"],"--profile","DEFAULT","-o","json"],capture_output=True,text=True)
    ok = '"space_id"' in p.stdout
    print(f"  {'OK ' if ok else 'MISS'} {v['title']}  ({v['space_id']})")
PY

echo "== Genie space tag assignments (geniespaces) =="
python3 - <<'PY'
import json,subprocess,urllib.parse
s=json.load(open("genie_domains/spaces_created.json"))
for k,v in s.items():
    tk=urllib.parse.quote(v["tag"])
    ep=f"/api/2.0/entity-tag-assignments/geniespaces/{v['space_id']}/tags/{tk}"
    p=subprocess.run(["databricks","api","get",ep,"--profile","DEFAULT"],capture_output=True,text=True)
    print(f"  {'OK ' if v['tag'] in p.stdout else 'MISS'} {v['tag']:22} <- {v['title']}")
PY

echo "== Grounding objects in jmrdemo.synth_genie =="
databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json \
 "{\"warehouse_id\":\"$WH\",\"catalog\":\"jmrdemo\",\"statement\":\"SELECT 'functions' kind, count(*) n FROM jmrdemo.information_schema.routines WHERE routine_schema='synth_genie' UNION ALL SELECT 'tables_and_metric_views', count(*) FROM jmrdemo.information_schema.tables WHERE table_schema='synth_genie'\",\"wait_timeout\":\"30s\"}" 2>/dev/null \
 | python3 -c "import sys,json;d=json.load(sys.stdin);[print('  ',r[0],'=',r[1]) for r in d.get('result',{}).get('data_array',[]) or []]"

echo "== Governed tags =="
databricks api post /api/2.0/sql/statements --profile "$PROFILE" --json \
 "{\"warehouse_id\":\"$WH\",\"statement\":\"SHOW GOVERNED TAGS\",\"wait_timeout\":\"30s\"}" 2>/dev/null \
 | python3 -c "import sys,json;d=json.load(sys.stdin);[print('  ',r[0]) for r in d.get('result',{}).get('data_array',[]) or [] if r[0] in ('Orders and SOS','Loyalty and Rewards','Inventory and Waste','Workforce and Labor')]"
