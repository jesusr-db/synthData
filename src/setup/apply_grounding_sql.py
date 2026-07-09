# Databricks notebook source
# COMMAND ----------
# Apply genie_domains/01_grounding.sql headless: table/column COMMENTs, trusted SQL functions,
# and metric views in <catalog>.<prefix>genie. Statements are separated by lines containing only ;;;.
# COMMENT ON COLUMN on silver streaming tables is blocked by Lakeflow — those are best-effort (logged, not fatal).
import sys, os

_notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
_bundle_root = "/Workspace" + "/".join(_notebook_path.replace("/Workspace", "").split("/")[:-3])
if _bundle_root not in sys.path:
    sys.path.insert(0, _bundle_root)

try:
    catalog_name = dbutils.widgets.get("catalog_name")
except Exception:
    catalog_name = "jmrdemo"
try:
    schema_prefix = dbutils.widgets.get("schema_prefix")
except Exception:
    schema_prefix = "synth_"

print(f"[INFO] apply_grounding_sql: catalog={catalog_name}, schema_prefix={schema_prefix}")

# COMMAND ----------
sql_path = os.path.join(_bundle_root, "genie_domains", "01_grounding.sql")
with open(sql_path) as fh:
    txt = fh.read()

# The grounding SQL is authored against jmrdemo.synth_*; rewrite to the configured catalog/prefix.
txt = txt.replace("jmrdemo.synth_", f"{catalog_name}.{schema_prefix}")

stmts = [s.strip() for s in txt.split("\n;;;\n") if s.strip() and not s.strip().startswith("--")]
ok = warn = 0
for i, s in enumerate(stmts):
    label = s.split("\n")[0][:70]
    try:
        spark.sql(s)
        ok += 1
    except Exception as e:
        msg = str(e)
        # COMMENT ON COLUMN on silver streaming tables is expected to fail (Lakeflow restriction).
        if "STREAMING_TABLE_OPERATION_NOT_ALLOWED" in msg or "COMMENT ON COLUMN" in s.upper():
            warn += 1
            print(f"[WARN] ({i+1}/{len(stmts)}) skipped (streaming-table restriction): {label}")
        else:
            print(f"[ERR]  ({i+1}/{len(stmts)}) {label}\n       -> {msg[:300]}")
            raise

print(f"[OK] apply_grounding_sql complete — {ok} statements applied, {warn} best-effort skips")
