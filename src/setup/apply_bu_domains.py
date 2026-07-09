# Databricks notebook source
# COMMAND ----------
# Group the 11 Genie spaces under 4 Business-Unit governed-tag domains (Discover page).
# Runs AFTER build_genie_spaces (needs space_ids). Idempotent: tears down prior BU domains/tags first.
import sys, json, requests

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

from databricks.sdk import WorkspaceClient
from genie_domains import _domains
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
me = w.current_user.me()
owner_id = int(me.id)

print(f"[INFO] apply_bu_domains: catalog={catalog_name}, owner={me.user_name}")

# COMMAND ----------
# REST + SQL callables injected into the shared _domains module.
def post(path, body):
    r = requests.post(f"{workspace_url}{path}", headers=headers, json=body, timeout=60)
    if r.status_code not in (200, 201):
        print(f"[WARN] POST {path} -> {r.status_code} {r.text[:200]}")
        return {}
    return r.json()

def get(path):
    r = requests.get(f"{workspace_url}{path}", headers=headers, timeout=30)
    return r.json() if r.status_code == 200 else {}

def delete(path):
    r = requests.delete(f"{workspace_url}{path}", headers=headers, timeout=30)
    return r.status_code in (200, 204)

def run_sql(stmt, best_effort=False):
    try:
        spark.sql(stmt)
    except Exception as e:
        if best_effort:
            print(f"[WARN] sql skipped: {stmt[:70]} -> {str(e)[:150]}")
        else:
            raise

# COMMAND ----------
# Pull the {space_key: {tag, bu, space_id}} map from the upstream build_genie_spaces task.
try:
    spaces = dbutils.jobs.taskValues.get(taskKey="build_genie_spaces", key="spaces",
                                         default=None, debugValue=None)
except Exception:
    spaces = None
if not spaces:
    # Fallback: resolve space_ids by title straight from the API.
    listing = get("/api/2.0/genie/spaces").get("spaces", [])
    by_title = {s.get("title"): s.get("space_id") for s in listing}
    spaces = {k: {"tag": d["tag"], "bu": d["bu"], "space_id": by_title.get(d["title"], "")}
              for k, d in DOMAINS.items()}
print(f"[INFO] resolved {sum(1 for v in spaces.values() if v.get('space_id'))} space ids")

# Build the {space_key: [table identifiers]} map (rewrite catalog/prefix to match this workspace).
def remap(ident):
    return ident.replace("jmrdemo.synth_", f"{catalog_name}.{schema_prefix}")
space_tables = {k: [remap(t["identifier"]) for t in serialized(d)["data_sources"]["tables"]]
                for k, d in DOMAINS.items()}

# COMMAND ----------
# 1. Teardown any prior BU domains + governed tags + space tag-assignments (idempotent).
_domains.teardown(get, delete, run_sql, drop_tags=True, spaces=spaces)

# 2. Create governed tags (parents + children).
_domains.create_governed_tags(run_sql)

# 3. Apply child tags to each space's UC assets + to the space entity.
_domains.apply_tags_to_assets(run_sql, space_tables)
_domains.assign_tags_to_spaces(post, spaces)

# 4. Create + publish the Domain cards.
created = _domains.create_domain_cards(post, owner_id)

print("[OK] apply_bu_domains complete")
print(json.dumps(created, indent=2))
dbutils.notebook.exit(json.dumps(created))
