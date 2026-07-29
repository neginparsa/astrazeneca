#!/usr/bin/env bash
# Install Azure CLI + Databricks CLI on macOS (Homebrew).
set -euo pipefail

if ! command -v brew >/dev/null 2>&1; then
  echo "Homebrew is required. Install from https://brew.sh then re-run this script."
  exit 1
fi

echo "Installing Azure CLI..."
brew install azure-cli

echo "Installing Databricks CLI..."
brew tap databricks/tap
brew install databricks

echo "Done. Run:"
echo "  az login"
echo "  ./scripts/setup/check_prerequisites.sh"
