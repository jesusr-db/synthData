#!/usr/bin/env bash
# Create + publish the 4 Discover domains via API (maps each to its governed tag).
# Domains are published immediately (effective_draft=false). Idempotent-ish: re-running
# creates duplicates, so run teardown first if recreating.
set -uo pipefail
ME=$(databricks current-user me --profile DEFAULT -o json | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
mk(){ python3 - "$1" "$2" "$3" "$4" "$ME" <<'PY'
import sys,json,subprocess
tag,sub,desc,color,me=sys.argv[1:6]
body=json.dumps({"tag_key":tag,"subtitle":sub,"description":desc,
 "technical_owner_ids":[int(me)],"business_owner_ids":[int(me)],"icon":{"color":color,"name":"BASKET"}})
p=subprocess.run(["databricks","api","post","/api/2.0/domains","--profile","DEFAULT","--json",body],capture_output=True,text=True)
try:d=json.loads(p.stdout);print(f"  OK {tag} id={d['domain_id']} published={not d['effective_draft']}")
except:print(f"  ERR {tag}: {p.stdout[:160]}{p.stderr[:160]}")
PY
}
mk "Orders and SOS"      "Orders, revenue, channels & Speed-of-Service" "Orders, revenue, channels, SOS compliance and delivery across PizzaTel QSR stores." "#1B5E20"
mk "Loyalty and Rewards" "Membership, points, rewards & tiers"          "Loyalty membership, points, rewards, tiers, member vs non-member." "#6A1B9A"
mk "Inventory and Waste" "On-hand, par, waste & receiving"              "On-hand inventory, par/stockout, waste cost/category, receiving." "#B71C1C"
mk "Workforce and Labor" "Shifts, labor hours & productivity"          "Shifts, labor hours, overtime, headcount, sales per labor hour." "#0D47A1"
