# Enable Cursor Agent to run Git for you

The agent can run `git add`, `commit`, `push`, and `pull` **after GitHub accepts SSH auth** on this Mac. HTTPS fails in the agent because it cannot type your username/PAT interactively.

## One-time setup (about 2 minutes)

An SSH key was created (or will be created) at:

`~/.ssh/id_ed25519`

### Step 1 — Add the key to GitHub

1. Copy your **public** key (run in Terminal):

   ```bash
   cat ~/.ssh/id_ed25519.pub
   ```

2. Open [GitHub → SSH keys → New](https://github.com/settings/ssh/new)
3. **Title:** `Cursor Mac`
4. **Key:** paste the full line (`ssh-ed25519 AAAA...`)
5. Click **Add SSH key**

### Step 2 — Switch this repo to SSH and push

```bash
cd "/Users/neginnickparsa/Projects/Databricks Magnolia"
git remote set-url origin git@github.com:neginparsa/astrazeneca.git
ssh -T git@github.com
git push -u origin main
```

You should see: `Hi neginparsa! You've successfully authenticated...`  
Then push should upload `notebooks/`, etc.

**Or run the helper script** (same steps, interactive):

```bash
chmod +x scripts/setup/enable_agent_git.sh
./scripts/setup/enable_agent_git.sh
```

### Step 3 — Databricks

Git folder **astra** → **Git → Pull** → open `notebooks/QUICKSTART_setup_and_run`.

---

## What the agent can do after this

When you say **push**, **commit**, or **pull**, the agent can run Git in this project without your PAT.

You may still see a **Cursor approval** prompt for pushes to GitHub — click **Approve**.

---

## Security

- Never paste **PATs** or **private** keys (`id_ed25519` without `.pub`) in chat.
- Only the **`.pub`** file goes to GitHub.
- Revoke old PATs if you exposed them earlier.
