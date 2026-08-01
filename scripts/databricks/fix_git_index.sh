#!/usr/bin/env bash
# Run in Databricks Git folder Web Terminal when you see:
#   fatal: unknown index entry format 0x...
set -euo pipefail

REPO="${1:-.}"
cd "$REPO"

if [[ ! -d .git ]]; then
  echo "Not a git repo: $(pwd)"
  exit 1
fi

echo "Fixing corrupted index in $(pwd) ..."
rm -f .git/index
git reset
echo ""
git status
echo ""
echo "Done. Try Git Pull in the UI."
