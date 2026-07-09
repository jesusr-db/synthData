#!/usr/bin/env python3
"""Build/refresh the 11 QSR Genie spaces (local CLI wrapper).

Thin wrapper over genie_domains/_spaces.py — the same module the setup-job notebook
(src/setup/build_genie_spaces.py) imports, so definitions never drift.
Idempotent: matches existing spaces by title and UPDATEs them, else CREATEs.
Writes each serialized_space to genie_domains/space_<key>.json for the record.

Usage: python3 genie_domains/build_spaces.py
"""
import os, sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _spaces

WH = "d56091a1171f30ff"
PROFILE = "DEFAULT"
PARENT = "/Users/jesus.rodriguez@databricks.com"


def main():
    results = _spaces.build_all(WH, PARENT, profile=PROFILE)
    ok = sum(1 for r in results.values() if r["ok"])
    print(f"\nDONE: {ok}/{len(results)} spaces built")
    print("WROTE", os.path.join(os.path.dirname(os.path.abspath(__file__)), "spaces_created.json"))


if __name__ == "__main__":
    main()
