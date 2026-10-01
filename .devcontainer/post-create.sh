#!/usr/bin/env bash
set -uo pipefail  # no -e: log step failures but always finish (never fail container creation)

# Codespaces/devcontainer bootstrap (headless, runs once at container creation).
# Snowflake credentials come from Codespaces secrets injected as env vars and read
# by profiles.yml via env_var(), so there is no interactive .env onboarding here
# (that lives in start_dbt.ps1 / start_dbt.sh for local machines). Mirrors their
# Fusion-only setup: install Fusion, sync the scripts/ Python env, install packages.

git config core.hooksPath .githooks

# Install the latest dbt Fusion engine, matching the VS Code extension.
# The installer's target auto-detection is unreliable, so pass it explicitly.
case "$(uname -m)" in arm64|aarch64) arch_part="aarch64" ;; *) arch_part="x86_64" ;; esac
case "$(uname -s)" in Darwin) os_part="apple-darwin" ;; *) os_part="unknown-linux-gnu" ;; esac
fusion_target="${arch_part}-${os_part}"
curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --target "$fusion_target" --update \
    || echo "[warn] dbt Fusion installer returned non-zero (continuing)"
export PATH="$HOME/.local/bin:$PATH"
command -v dbt >/dev/null 2>&1 && echo "[OK] $(dbt --version 2>&1 | head -1)" || echo "[warn] dbt not on PATH after install"

# Python tooling for the helper scripts in scripts/ (optional - not needed for dbt).
# Best-effort: a failure here must not fail container creation.
if ! command -v uv >/dev/null 2>&1; then
    curl -LsSf https://astral.sh/uv/install.sh | sh || echo "[warn] uv install failed"
    export PATH="$HOME/.local/bin:$PATH"
fi
if command -v uv >/dev/null 2>&1; then
    uv sync || echo "[warn] uv sync failed (Python helper scripts unavailable)"
fi

# dbt packages
if command -v dbt >/dev/null 2>&1; then
    dbt deps || echo "[warn] dbt deps failed - run 'dbt deps' once credentials are available"
else
    echo "[warn] dbt unavailable - skipping dbt deps"
fi

if [ -n "${SNOWFLAKE_PAT:-}" ]; then
    echo "Codespaces bootstrap complete. Snowflake PAT detected."
else
    echo "Codespaces bootstrap complete. Add Snowflake secrets before running dbt debug."
fi
