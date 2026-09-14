#!/usr/bin/env bash
# start_dbt.sh - macOS/Linux equivalent of start_dbt.ps1
#
# Bootstraps the dev environment. Auto-runs when you open a terminal in the
# VS Code workspace (see dbt-ncl-analytics.code-workspace).
#
# dbt runs on the Fusion engine (installed to ~/.local/bin), NOT a Python
# package. The .venv exists only for the Python tooling in scripts/.

# Fusion is unpinned. CI installs latest and the VS Code extension manages the
# local version. This script installs latest only when dbt is missing.

actions=()
install_dir="$HOME/.local/bin"
# Installer platform target (its auto-detection is currently unreliable, so we pass it).
case "$(uname -s)" in
    Darwin) os_part="apple-darwin" ;;
    *)      os_part="unknown-linux-gnu" ;;
esac
case "$(uname -m)" in
    arm64|aarch64) arch_part="aarch64" ;;
    *)             arch_part="x86_64" ;;
esac
fusion_target="${arch_part}-${os_part}"

# Ensure the Fusion install dir is on PATH for this session.
case ":$PATH:" in
    *":$install_dir:"*) ;;
    *) export PATH="$install_dir:$PATH" ;;
esac

# ---------------------------------------------------------------------------
# 1. Git hooks
# ---------------------------------------------------------------------------
echo "Configuring Git hooks..."
if [ "$(git config core.hooksPath)" != ".githooks" ]; then
    git config core.hooksPath .githooks
    echo "[OK] Git hooks configured to use .githooks directory"
else
    echo "[OK] Git hooks already configured"
fi
echo ""

# ---------------------------------------------------------------------------
# 2. Commit signing check
# ---------------------------------------------------------------------------
echo "Checking commit signing..."
gpg_format=$(git config gpg.format)
signing_key=$(git config user.signingkey)
auto_sign=$(git config commit.gpgsign)
if [ "$gpg_format" = "ssh" ] && [ -n "$signing_key" ] && [ "$auto_sign" = "true" ]; then
    echo "[OK] Commit signing configured"
else
    echo "[WARNING] Commit signing is not configured"
    echo "  This repository requires signed commits for branch protection."
    echo "  See CONTRIBUTING.md 'Setting Up Commit Signing' for setup instructions."
    actions+=("Set up commit signing (see CONTRIBUTING.md)")
fi
echo ""

# ---------------------------------------------------------------------------
# 3. dbt Fusion engine
# ---------------------------------------------------------------------------
echo "Checking dbt Fusion engine..."
install_fusion() {
    curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --target "$fusion_target"
}

if ! command -v dbt &> /dev/null; then
    echo "[INFO] Installing dbt Fusion (latest)..."
    install_fusion
    export PATH="$install_dir:$PATH"
else
    echo "[OK] dbt Fusion available; the VS Code extension manages updates"
fi

if ! command -v dbt &> /dev/null; then
    echo "[WARNING] dbt Fusion not available - install manually:"
    echo "  curl -fsSL https://public.cdn.getdbt.com/fs/install/install.sh | sh -s -- --target $fusion_target"
    actions+=("Install dbt Fusion (see CONTRIBUTING.md)")
else
    echo "  $(dbt --version 2>&1 | head -1)"
fi
echo ""

# ---------------------------------------------------------------------------
# 4. Legacy dbt-core cleanup
# ---------------------------------------------------------------------------
# A dbt-core install puts a dbt entrypoint in .venv/bin. Fusion does not
# (it lives in ~/.local/bin), so this marks a pre-Fusion machine.
if [ -f ".venv/bin/dbt" ]; then
    echo "Clearing legacy dbt-core artifacts..."
    for d in target logs; do
        if [ -d "$d" ]; then rm -rf "$d"; echo "  Removed $d/"; fi
    done
    echo "  dbt-core will be pruned from .venv by uv sync below"
    echo ""
fi

# ---------------------------------------------------------------------------
# 5. Python venv for the helper scripts in scripts/ (optional - not needed for dbt)
# ---------------------------------------------------------------------------
echo "Syncing Python tooling for scripts/..."
if ! command -v uv &> /dev/null; then
    echo "[INFO] uv not found - installing..."
    curl -LsSf https://astral.sh/uv/install.sh | sh || echo "[WARNING] uv install failed"
    export PATH="$HOME/.local/bin:$PATH"
fi
if command -v uv &> /dev/null; then
    if uv sync; then
        [ -f ".venv/bin/activate" ] && source .venv/bin/activate
        echo "[OK] Python tooling ready"
    else
        echo "[WARNING] uv sync failed - scripts/ Python tools may be incomplete"
        actions+=("Re-run 'uv sync' for the Python helper scripts")
    fi
else
    echo "[WARNING] uv unavailable - scripts/ Python tools skipped (install: curl -LsSf https://astral.sh/uv/install.sh | sh)"
    actions+=("Install uv to run the Python helper scripts (optional)")
fi
echo ""

# Disable AWS metadata service checks (prevents connection pool warnings on Azure)
export AWS_EC2_METADATA_DISABLED=true

# Interactive first-run setup: prompt for connection details and write .env.
new_dbt_env_file() {
    local path="${1:-.env}"
    echo ""
    echo "No .env found - let's set up your Snowflake connection."
    local account user role warehouse choice secret
    read -r -p "Snowflake account identifier: " account
    read -r -p "Snowflake username: " user
    read -r -p "Snowflake role: " role
    read -r -p "Snowflake warehouse: " warehouse
    echo ""
    echo "Authentication method:"
    echo "  1) Browser SSO (externalbrowser)   [default, recommended]"
    echo "  2) Programmatic Access Token (PAT)"
    echo "  3) Account password + MFA"
    echo "     (PAT -> SNOWFLAKE_PAT/token; password -> SNOWFLAKE_PASSWORD)"
    read -r -p "Selection [1]: " choice
    [ -z "$choice" ] && choice=1

    # Only write keys the user actually provided - no assumed defaults.
    {
        echo "# Generated by start_dbt.sh onboarding. Never commit this file."
        [ -n "$account" ]   && echo "SNOWFLAKE_ACCOUNT=$account"
        [ -n "$user" ]      && echo "SNOWFLAKE_USER=$user"
        [ -n "$role" ]      && echo "SNOWFLAKE_ROLE=$role"
        [ -n "$warehouse" ] && echo "SNOWFLAKE_WAREHOUSE=$warehouse"
    } > "$path"
    case "$choice" in
        2) read -r -s -p "Paste your PAT: " secret; echo ""; echo "SNOWFLAKE_PAT=$secret" >> "$path" ;;
        3) read -r -s -p "Account password: " secret; echo ""; echo "SNOWFLAKE_PASSWORD=$secret" >> "$path"; echo "SNOWFLAKE_AUTHENTICATOR=username_password_mfa" >> "$path" ;;
        *) echo "SNOWFLAKE_AUTHENTICATOR=externalbrowser" >> "$path" ;;
    esac

    # Load into the current shell so dbt works immediately (no shell interpretation of values).
    while IFS= read -r line || [ -n "$line" ]; do
        [[ "$line" =~ ^#.*$ || -z "$line" ]] && continue
        [[ "$line" == *"="* ]] && export "${line%%=*}=${line#*=}"
    done < "$path"
    echo "[OK] Created $path"
}

# ---------------------------------------------------------------------------
# 6. Load environment variables from .env
# ---------------------------------------------------------------------------
# Fusion auto-loads .env, but we also load it here for the Python scripts and to
# surface the active connection details.
has_snowflake_env=false
if [ -n "$SNOWFLAKE_ACCOUNT" ] && [ -n "$SNOWFLAKE_USER" ] && { [ -n "$SNOWFLAKE_PAT" ] || [ -n "$SNOWFLAKE_PASSWORD" ] || [ "$SNOWFLAKE_AUTHENTICATOR" = "externalbrowser" ]; }; then
    has_snowflake_env=true
fi

echo "Loading environment variables from .env..."
if [ -f ".env" ]; then
    env_count=0
    while IFS= read -r line || [ -n "$line" ]; do
        [[ "$line" =~ ^#.*$ || -z "$line" ]] && continue
        if [[ "$line" == *"="* ]]; then
            key="${line%%=*}"
            value="${line#*=}"
            export "$key=$value"
            ((env_count++))
        fi
    done < ".env"
    echo "[OK] Loaded $env_count environment variables"

    if grep -v '^#' ".env" | grep -q '^[^=]*=.*your-.*-here'; then
        echo "[WARNING] .env still contains placeholder values"
        actions+=("Update credentials in .env, then open a new terminal (Ctrl+\`)")
    else
        [ -n "$SNOWFLAKE_ACCOUNT" ] && echo "  SNOWFLAKE_ACCOUNT: ${SNOWFLAKE_ACCOUNT:0:10}..."
        [ -n "$SNOWFLAKE_USER" ] && echo "  SNOWFLAKE_USER: $SNOWFLAKE_USER"
        [ -n "$SNOWFLAKE_ROLE" ] && echo "  SNOWFLAKE_ROLE: $SNOWFLAKE_ROLE"
        [ -n "$SNOWFLAKE_WAREHOUSE" ] && echo "  SNOWFLAKE_WAREHOUSE: $SNOWFLAKE_WAREHOUSE"
        [ -n "$SNOWFLAKE_PAT" ] && echo "  SNOWFLAKE_PAT: ${SNOWFLAKE_PAT:0:8}..."
        [ -n "$SNOWFLAKE_PASSWORD" ] && echo "  SNOWFLAKE_PASSWORD: [set]"
        [ -n "$SNOWFLAKE_AUTHENTICATOR" ] && echo "  SNOWFLAKE_AUTHENTICATOR: $SNOWFLAKE_AUTHENTICATOR"
    fi
elif [ "$has_snowflake_env" = true ]; then
    echo "[OK] No .env file found - using existing environment variables"
elif [ -t 0 ]; then
    # Interactive terminal with no .env - walk the user through setup.
    new_dbt_env_file
elif [ -f "env.example" ]; then
    # Non-interactive (e.g. automation) - fall back to the template.
    cp env.example .env
    echo "[WARNING] No .env file found - created from template"
    actions+=("Update credentials in .env, then open a new terminal (Ctrl+\`)")
else
    echo "[WARNING] No .env file found and no env.example template"
    actions+=("Update credentials in .env, then open a new terminal (Ctrl+\`)")
fi
echo ""

# ---------------------------------------------------------------------------
# 7. dbt packages - install if missing, or if packages.yml changed since last install
# ---------------------------------------------------------------------------
# Drop a stale dbt-core-format package-lock.yml. Fusion flags it (dbt1041 "Old
# format package-lock.yml") and its pins can clash with packages.yml (dbt1005).
# The old format lacks the per-package `name:` field Fusion writes, so detect by
# its absence; `dbt deps` then regenerates a current, in-sync lock.
need_deps=false
if [ -f "package-lock.yml" ] && ! grep -q '^[[:space:]]*name:' package-lock.yml; then
    rm -f package-lock.yml
    echo "[INFO] Removed old-format package-lock.yml - dbt deps will regenerate it"
    need_deps=true
fi
if [ ! -d "dbt_packages" ]; then
    need_deps=true
elif [ -f "packages.yml" ] && [ "packages.yml" -nt "dbt_packages" ]; then
    need_deps=true
fi
if [ "$need_deps" = true ]; then
    echo "Installing dbt packages (dbt deps)..."
    dbt deps || actions+=("Run 'dbt deps' to install dbt packages")
    echo ""
fi

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
if [ ${#actions[@]} -gt 0 ]; then
    echo "To finish setup:"
    for action in "${actions[@]}"; do
        echo "  -> $action"
    done
else
    echo "Ready! dbt runs on the Fusion engine."
    echo "Try: dbt debug"
fi
