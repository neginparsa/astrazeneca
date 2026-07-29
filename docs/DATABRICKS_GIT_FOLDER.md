# Connect this project to Databricks (Git folder)

Use a **Git folder** in your Databricks home (recommended UI). Free Edition works with GitHub.

## Part A — Put this project on GitHub (one time, on your Mac)

### 1. Initialize and push (Terminal)

```bash
cd "/Users/neginnickparsa/Projects/Databricks Magnolia"
git init
git add .
git commit -m "Initial Magnolia Pharma lakehouse project for Databricks Free Edition"
```

### 2. Create an empty GitHub repo

1. Go to [github.com/new](https://github.com/new)
2. Name: e.g. `magnolia-pharma-lakehouse`
3. **Private** recommended
4. Do **not** add README, .gitignore, or license (repo stays empty)
5. Click **Create repository**

### 3. Push your code

Replace `YOUR_USER` and `YOUR_REPO` with yours:

```bash
git branch -M main
git remote add origin https://github.com/YOUR_USER/YOUR_REPO.git
git push -u origin main
```

Use GitHub **Personal Access Token** as password if prompted (Settings → Developer settings → Fine-grained or classic token with `repo` scope).

---

## Part B — Create a Git folder in Databricks Free Edition

### 1. Open your home folder

1. In Databricks, left sidebar → **Workspace**
2. Click your **username** (home), **not** `/Repos` first — new flow uses Git folders on home

### 2. Add Git folder

1. On the home folder, click **Add** (or **Create**) → **Git folder**  
   (If you only see **Repo**, choose **Git folder** / **Clone from Git** when offered.)
2. **Git repository URL**: `https://github.com/YOUR_USER/YOUR_REPO.git`
3. **Git provider**: GitHub
4. **Branch**: `main`
5. **Path** / folder name: e.g. `magnolia-pharma-lakehouse`
6. Authenticate with GitHub if asked (OAuth or PAT)
7. **Create**

After sync, you should see:

```text
/Users/you/magnolia-pharma-lakehouse/
  notebooks/QUICKSTART_setup_and_run.py
  config/env.yaml
  docs/FREE_EDITION.md
  ...
```

### 3. Open and run the notebook

1. Navigate to **`notebooks/QUICKSTART_setup_and_run`**
2. Compute: **Serverless**
3. Widget **catalog** = **`main`**
4. **Run all**

Config loads from `config/env.yaml` in the repo automatically.

---

## Part C — Stay in sync

| Action | Where |
|--------|--------|
| Edit in Cursor, push to GitHub | Mac: `git add`, `git commit`, `git push` |
| Pull into Databricks | Git folder → **Git** → **Pull** (or **Sync**) |

---

## Troubleshooting

| Issue | Fix |
|-------|-----|
| No **Git folder** option | Try **Add → Repo** under home; or update workspace — UI varies by rollout |
| Auth failed | GitHub PAT with `repo`; or connect GitHub in Databricks **User Settings → Linked accounts** |
| `config/env.yaml` not found in notebook | `%cd` to repo root, or set env `MAGNOLIA_CONFIG` to `/Workspace/Users/you/.../config/env.yaml` |
| Still see old **Repos** path | Optional: **Workspace → Repos** — clone there instead; same URL and steps |

---

## Legacy: Repo under `/Repos`

If your workspace only supports the classic flow:

1. **Workspace → Repos → Add repo**
2. Same Git URL and branch
3. Path becomes `/Repos/you/magnolia-pharma-lakehouse`

Notebooks and config work the same.
