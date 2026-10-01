# Developing in GitHub Codespaces

Codespaces gives you a cloud dev environment with no local install. Add your
Snowflake secrets once, create a codespace, and dbt (on the Fusion engine) is set
up for you - Fusion, the Python tooling, and dbt packages are installed on
creation.

## 1. Add your Snowflake secrets (one-time)

Codespaces runs headless, so it authenticates to Snowflake with a **Programmatic
Access Token (PAT)** - browser SSO can't complete a redirect inside a remote
codespace.

1. Create a PAT in Snowflake for your user (Snowsight -> your profile ->
   Programmatic access tokens).
2. Add these as **your** Codespaces secrets, scoped to **this repository**:

   | Secret | Example |
   |--------|---------|
   | `SNOWFLAKE_ACCOUNT` | `ORG-ACCOUNT` |
   | `SNOWFLAKE_USER` | `your.name@nhs.net` |
   | `SNOWFLAKE_ROLE` | your role (e.g. `ANALYST`) |
   | `SNOWFLAKE_WAREHOUSE` | your warehouse |
   | `SNOWFLAKE_PAT` | the token from step 1 |

You can add them either way:

- **GitHub UI** — [Settings -> Codespaces -> Secrets](https://github.com/settings/codespaces) ->
  *New secret*. Set the name and value, and under **Repository access** select
  `wnl-icb-analytics/dbt-analytics`. The repository association is what makes the
  secret available to codespaces created from this repo.
- **Helper script** (from a working local `.env`):
  ```powershell
  .\scripts\setup_codespaces_secrets.ps1
  ```
  Uses the GitHub CLI to upload the supported Snowflake values as user-level
  Codespaces secrets scoped to this repository.

## 2. Create the codespace

GitHub -> **Code** -> **Codespaces** -> **New** (use *New with options* to pick a
branch). Wait for setup to finish. On creation, `.devcontainer/post-create.sh`
installs the dbt Fusion engine, syncs the `scripts/` Python tooling, and runs
`dbt deps`.

## 3. Verify

```bash
dbt debug      # checks the Snowflake connection
dbt compile    # parses + statically analyses the project
```

## How it works (no `.env` needed)

Codespaces injects your secrets as environment variables, and `profiles.yml`
reads them with `env_var()`. With `SNOWFLAKE_PAT` set, dbt authenticates via
`programmatic_access_token` (the PAT goes in the `token` field) - so there's no
`.env` file and no browser login inside the codespace.

## Troubleshooting

- **Setup didn't finish / postCreate failed** — `Cmd/Ctrl+Shift+P` ->
  *Codespaces: Rebuild Container* re-runs setup; *View Creation Log* shows
  details. You can also run `./start_dbt.sh` in a terminal to install Fusion +
  uv on demand.
- **`dbt: command not found`** — open a fresh terminal (PATH picks up
  `~/.local/bin`), or run `./start_dbt.sh`.
- **"dbt extension session expired / Sign In"** — that's the dbt VS Code
  extension's platform login. It's optional and does not affect the `dbt` CLI.
- **No secrets set** — `dbt debug` will fail to connect. Add the secrets above
  (and rebuild, or open a new terminal so they're picked up).

See [CONTRIBUTING.md](../CONTRIBUTING.md) for full contributor setup, and
[dbt-onboarding.vercel.app](https://dbt-onboarding.vercel.app/) for the VS Code
extension and dbt learning.
