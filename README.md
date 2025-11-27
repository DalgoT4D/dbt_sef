# dbt_sef — Setup & run (uv + dbt)

This README explains how to set up the project's Python environment using uv, configure dbt's profiles.yml, and run dbt commands. Keep credentials and secrets out of version control.

## Prerequisites
- macOS (or Linux)
- Python 3.8+
- git
- pipx (recommended) or pip

## 1) Install uv

https://docs.astral.sh/uv/getting-started/installation/

## 2) Install project dependencies
- From the project root (where pyproject.toml lives):
  - `uv install`   # or `uv sync` depending on uv version
- This creates a managed virtual environment and installs dependencies declared in the project.

## 3) Running commands inside the uv environment
- Either activate the created venv (if present):
  - `source .venv/bin/activate`
- Or run commands through uv so the proper environment is used, e.g.:
  - `uv run dbt -- debug`

## 4) Configure dbt profiles.yml
Use a project-level profiles.yml (recommended for reproducible CI).
- Place a profiles.yml in the project root and tell dbt to use it:
  - `cp profiles.example.yml profiles.yml`
  - `export DBT_PROFILES_DIR="$(pwd)"`

## 5) Validate configuration
- From project root (use uv run or activated venv):
  - `uv run dbt -- debug`
  - or `dbt debug`

## 6) Common dbt workflow
- Install package dependencies:
  - `uv run dbt -- deps`
- Load seeds (if any):
  - `uv run dbt -- seed`
- Run models:
  - `uv run dbt -- run`
- Run tests:
  - `uv run dbt -- test`
- Generate docs:
  - `uv run dbt -- docs generate`
  - `uv run dbt -- docs serve`

## Troubleshooting
- If dbt can't find profiles.yml, confirm DBT_PROFILES_DIR or copy profiles.yml to ~/.dbt/profiles.yml.
- Use `dbt debug` to surface connection/auth problems.
- Check network/firewall and service account permissions for cloud warehouses.

## Resources
- dbt docs: https://docs.getdbt.com
- dbt community: https://community.getdbt.com
