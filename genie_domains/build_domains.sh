#!/usr/bin/env bash
# Apply the 4 BU governed-tag domains + 11 child Genie-space domains (Discover page).
# Thin wrapper over build_domains.py, which shares logic with the setup-job notebook
# (src/setup/apply_bu_domains.py) so there is no drift. Run build_spaces.py first.
set -uo pipefail
cd "$(dirname "$0")/.."
python3 genie_domains/build_domains.py apply
