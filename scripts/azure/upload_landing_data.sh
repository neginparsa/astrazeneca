#!/usr/bin/env bash
# Upload local-data/landing to ADLS. Set STORAGE and RG or pass as env vars.
set -euo pipefail

RG="${RG:-rg-magnolia-pharma}"
STORAGE="${STORAGE:?Set STORAGE=your_storage_account_name}"

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
LANDING="$ROOT/local-data/landing"

if [[ ! -d "$LANDING" ]]; then
  echo "Run: python3 scripts/generate_sample_data.py"
  exit 1
fi

KEY=$(az storage account keys list -g "$RG" -n "$STORAGE" --query '[0].value' -o tsv)

for zone in claims specialty_rx prior_auth crm inventory; do
  shopt -s nullglob
  for f in "$LANDING/$zone"/*.csv; do
    base=$(basename "$f")
    echo "Uploading $base -> landing/$zone/"
    az storage fs file upload \
      --account-name "$STORAGE" \
      --account-key "$KEY" \
      --file-system lakehouse \
      --path "landing/$zone/$base" \
      --source "$f" \
      --output none
  done
done

echo "Upload complete."
