# Databricks notebook source
# COMMAND ----------
# Build/refresh the 11 QSR Genie spaces (best-practice serialized_space v2), headless.
# Imports the SAME space definitions the local CLI wrapper (genie_domains/build_spaces.py) uses,
# via genie_domains/_spaces.py, so definitions never drift. Creates via REST; idempotent by title.
import sys, json, requests

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

from databricks.sdk import WorkspaceClient
from genie_domains._spaces import DOMAINS, serialized

try:
    catalog_name = dbutils.widgets.get("catalog_name")
except Exception:
    catalog_name = "jmrdemo"
try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"

w = WorkspaceClient()
workspace_url = w.config.host.rstrip("/")
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
token = ctx.apiToken().get()
headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
PARENT = f"/Users/{w.current_user.me().user_name}"

print(f"[INFO] build_genie_spaces: catalog={catalog_name}, schema_prefix={schema_prefix}, spaces={len(DOMAINS)}")

# COMMAND ----------
# Resolve a warehouse for space creation.
warehouses = list(w.warehouses.list())
warehouse = next((wh for wh in warehouses if wh.state and wh.state.value in ("RUNNING", "STOPPED")),
                 warehouses[0] if warehouses else None)
if warehouse is None:
    raise ValueError("No SQL warehouse found — create one before running setup.")
warehouse_id = warehouse.id
print(f"[INFO] Using warehouse: {warehouse.name} ({warehouse_id})")

# COMMAND ----------
def list_spaces_by_title():
    # MUST paginate — the list endpoint caps results per page, so a single call misses
    # prior spaces and the create/update-by-title check would create duplicates on re-run.
    out, token = {}, None
    for _ in range(200):
        params = {"page_size": 100}
        if token:
            params["page_token"] = token
        r = requests.get(f"{workspace_url}/api/2.0/genie/spaces", headers=headers, params=params, timeout=30)
        if r.status_code != 200:
            break
        j = r.json()
        for s in j.get("spaces", []):
            out[s.get("title")] = s.get("space_id")
        token = j.get("next_page_token")
        if not token:
            break
    return out

have = list_spaces_by_title()
results = {}
for key, d in DOMAINS.items():
    blob = json.dumps(serialized(d))
    if d["title"] in have:
        sid = have[d["title"]]
        body = {"serialized_space": blob, "title": d["title"], "description": d["description"]}
        r = requests.patch(f"{workspace_url}/api/2.0/genie/spaces/{sid}", headers=headers, json=body, timeout=60)
        action = "UPDATED"
    else:
        body = {"warehouse_id": warehouse_id, "serialized_space": blob,
                "title": d["title"], "description": d["description"], "parent_path": PARENT}
        r = requests.post(f"{workspace_url}/api/2.0/genie/spaces", headers=headers, json=body, timeout=60)
        action = "CREATED"
    ok = r.status_code in (200, 201)
    sid = (r.json().get("space_id", sid) if ok else "")
    results[key] = {"title": d["title"], "tag": d["tag"], "bu": d["bu"], "space_id": sid,
                    "action": action, "ok": ok}
    print(f"{action} {'OK ' if ok else 'ERR'} {d['title']} -> {sid or r.text[:200]}")

# COMMAND ----------
# Hand the {tag, bu, space_id} map to the downstream apply_bu_domains task.
dbutils.jobs.taskValues.set(key="spaces", value=results)
n_ok = sum(1 for v in results.values() if v["ok"])
print(f"[OK] build_genie_spaces complete — {n_ok}/{len(results)} spaces built")
dbutils.notebook.exit(json.dumps(results))
