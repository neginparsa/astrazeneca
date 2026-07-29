#!/usr/bin/env bash
# Creates minimal Azure resources for Magnolia Pharma Lakehouse.
# Prerequisites: az login, subscription selected.
set -euo pipefail

LOCATION="${LOCATION:-eastus}"
RG="${RG:-rg-magnolia-pharma}"
STORAGE="${STORAGE:-}"  # leave empty to auto-generate
WORKSPACE="${WORKSPACE:-dbw-magnolia-pharma}"

if ! az account show >/dev/null 2>&1; then
  echo "Run: az login"
  exit 1
fi

if [[ -z "$STORAGE" ]]; then
  STORAGE="stmag$(openssl rand -hex 3)"
fi

echo "Resource group: $RG ($LOCATION)"
az group create --name "$RG" --location "$LOCATION" --output none

echo "Storage account: $STORAGE (ADLS Gen2)"
az storage account create \
  --name "$STORAGE" \
  --resource-group "$RG" \
  --location "$LOCATION" \
  --sku Standard_LRS \
  --kind StorageV2 \
  --enable-hierarchical-namespace true \
  --output none

KEY=$(az storage account keys list -g "$RG" -n "$STORAGE" --query '[0].value' -o tsv)
az storage container create --name lakehouse --account-name "$STORAGE" --account-key "$KEY" --output none

for dir in claims specialty_rx prior_auth crm inventory _checkpoints; do
  az storage fs directory create \
    --file-system lakehouse \
    --name "$dir" \
    --account-name "$STORAGE" \
    --account-key "$KEY" \
    --output none 2>/dev/null || true
done

echo "Databricks workspace: $WORKSPACE"
az databricks workspace create \
  --name "$WORKSPACE" \
  --resource-group "$RG" \
  --location "$LOCATION" \
  --sku premium \
  --output none

URL=$(az databricks workspace show -g "$RG" -n "$WORKSPACE" --query workspaceUrl -o tsv)

cat <<EOF

=== Created ===
Resource group:     $RG
Storage account:    $STORAGE
Container:          lakehouse
Databricks URL:     https://$URL

Update config/env.azure.yaml.template:
  storage_account: $STORAGE
  landing_base: abfss://lakehouse@${STORAGE}.dfs.core.windows.net/landing
  checkpoint_base: abfss://lakehouse@${STORAGE}.dfs.core.windows.net/_checkpoints

Next: open the workspace, complete Unity Catalog metastore setup in the UI,
then copy config/env.azure.yaml.template to config/env.yaml and upload data:
  ./scripts/azure/upload_landing_data.sh

EOF
