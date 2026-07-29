#!/usr/bin/env bash
# One-time setup so Cursor Agent can run git push/pull without HTTPS prompts.
set -euo pipefail

KEY="$HOME/.ssh/id_ed25519"
REPO="git@github.com:neginparsa/astrazeneca.git"
PROJECT="/Users/neginnickparsa/Projects/Databricks Magnolia"

if [[ ! -f "${KEY}.pub" ]]; then
  mkdir -p "$HOME/.ssh"
  chmod 700 "$HOME/.ssh"
  ssh-keygen -t ed25519 -C "neginparsa-cursor" -f "$KEY" -N ""
fi

echo ""
echo "=== Add this SSH public key to GitHub ==="
echo "Open: https://github.com/settings/ssh/new"
echo "Title: Cursor Mac (or any name)"
echo "Key type: Authentication Key"
echo ""
cat "${KEY}.pub"
echo ""
echo "After saving on GitHub, press Enter to test and push..."
read -r _

ssh -T git@github.com || true

cd "$PROJECT"
git remote set-url origin "$REPO"
git push -u origin main

echo ""
echo "Done. Agent can now run: git push, git pull, git commit (when you ask)."
