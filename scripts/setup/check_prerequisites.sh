#!/usr/bin/env bash
set -euo pipefail

ok=true
check() {
  if command -v "$1" >/dev/null 2>&1; then
    echo "[ok] $1 — $($1 --version 2>&1 | head -1)"
  else
    echo "[missing] $1 — run scripts/setup/install_prerequisites.sh"
    ok=false
  fi
}

check az
check databricks
command -v python3 >/dev/null && echo "[ok] python3 — $(python3 --version)" || { echo "[missing] python3"; ok=false; }

if $ok; then
  echo "Local prerequisites look good for Path 2 (Azure)."
else
  echo "You can still use Path 1 (Databricks Free + QUICKSTART notebook) without these tools."
fi
