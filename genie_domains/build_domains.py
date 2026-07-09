#!/usr/bin/env python3
"""Local runner: apply OR tear down the 4 BU governed-tag domains + 11 child spaces.

Thin CLI-backed wrapper over the shared genie_domains/_domains.py module — the SAME logic the
setup-job notebook (src/setup/apply_bu_domains.py) and destroy notebook run headless, so there is
no drift. Reads space ids from genie_domains/spaces_created.json (written by build_spaces.py).

Usage:
  python3 genie_domains/build_domains.py            # apply (teardown-then-recreate; idempotent)
  python3 genie_domains/build_domains.py teardown   # remove all BU domains + governed tags
"""
import json, os, subprocess, sys, time

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)
sys.path.insert(0, ROOT)
sys.path.insert(0, HERE)

from genie_domains import _domains
from genie_domains._spaces import DOMAINS, serialized
from runsql import run as _sqlrun

PROFILE = "DEFAULT"


def _run(args):
    return subprocess.run(["databricks"] + args + ["--profile", PROFILE], capture_output=True, text=True)

def post(path, body):
    r = _run(["api", "post", path, "--json", json.dumps(body)])
    if r.returncode != 0 or not r.stdout.strip():
        print(f"[ERR] POST {path} {(r.stderr or r.stdout)[:120]!r}")
        return {}
    try:    return json.loads(r.stdout)
    except Exception:  return {}

def get(path):
    r = _run(["api", "get", path])
    try:    return json.loads(r.stdout) if r.returncode == 0 else {}
    except Exception:  return {}

def delete(path):
    return _run(["api", "delete", path]).returncode == 0

def sql(stmt, best_effort=False):
    st, info = _sqlrun(stmt)
    if st != "OK" and not best_effort:
        raise RuntimeError(f"{stmt[:50]} -> {info}")
    if st != "OK":
        print(f"[WARN] {stmt[:45]} -> {str(info)[:70]}")


def _owner_id():
    return int(json.loads(_run(["current-user", "me", "-o", "json"]).stdout)["id"])


def main():
    action = sys.argv[1] if len(sys.argv) > 1 else "apply"
    spaces = json.load(open(os.path.join(HERE, "spaces_created.json")))
    if action == "teardown":
        _domains.teardown(get, delete, sql, drop_tags=True, spaces=spaces)
        print("Teardown complete — BU domains + governed tags + space tag-assignments removed.")
        return

    space_tables = {k: [t["identifier"] for t in serialized(d)["data_sources"]["tables"]]
                    for k, d in DOMAINS.items()}
    print("== teardown prior =="); _domains.teardown(get, delete, sql, drop_tags=True, spaces=spaces)
    print("== create governed tags =="); _domains.create_governed_tags(sql); time.sleep(3)
    print("== tag UC assets =="); _domains.apply_tags_to_assets(sql, space_tables)
    print("== tag space entities =="); _domains.assign_tags_to_spaces(post, spaces)
    print("== create domain cards =="); created = _domains.create_domain_cards(post, _owner_id())
    json.dump(created, open(os.path.join(HERE, "domains_created.json"), "w"), indent=2)
    np = sum(1 for v in created.values() if v["domain_id"])
    nc = sum(1 for v in created.values() for c in v["children"].values() if c)
    print(f"\nCREATED {np}/4 BU parents, {nc}/11 children -> domains_created.json")


if __name__ == "__main__":
    main()
