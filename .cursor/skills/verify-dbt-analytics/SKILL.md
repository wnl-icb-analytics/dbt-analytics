---
name: verify-dbt-analytics
description: "Drive the WNL ICB dbt-analytics project from the CLI: Fusion compile, selected model build/test, and GitHub Actions static checks. Use when proving a model or test change, before opening a PR, or when a cloud agent needs to verify dbt behaviour without a web UI."
---

# Verify dbt-analytics

This is a dbt project. The user-facing surface is the Fusion CLI plus the static scripts GitHub Actions runs on every pull request. There is no web app to click. Analysts compile, build and test models, then rely on `dbt-code-quality.yml` and `model-ownership.yml` before merge-queue warehouse validation.

Read `features/README.md` and the matching feature file before driving. Prove the mapped entry points in that file, not a convenient substitute.

## Launch

There is no long-lived server. Launch means Fusion and `dbt_packages` are present, then each drive is a short CLI process.

From the repository root:

```bash
export PATH="$HOME/.local/bin:$PATH"
export VERIFY_DBT_RUN_ID="${VERIFY_DBT_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
.cursor/skills/verify-dbt-analytics/bin/control-dbt-analytics launch
```

Ready when launch prints `launch: ready` and `fusion:` starts with `dbt-fusion`. Packages are ready when `dbt_packages/dbt_utils` exists.

Do not run `start_dbt.sh` for verification. It can copy `env.example` to `.env` or prompt for Snowflake credentials. Do not create `.env`, do not add Snowflake MCP, and do not invent a new connector. Use whatever `SNOWFLAKE_*` variables the environment already has, and only as booleans in doctor output.

Linux equivalent of `.\start_dbt.ps1` for Fusion plus packages is this launch command. Linux equivalent of `.\build_changed.ps1` is `dbt compile|build|test -s <selection>` with files from `git diff origin/main...HEAD`.

Teardown: `control-dbt-analytics cleanup`. That removes the scratch directory only.

## Doctor

Run this first whenever anything looks off:

```bash
.cursor/skills/verify-dbt-analytics/bin/control-dbt-analytics doctor
```

Require:

- `fusion: ok` with a `dbt-fusion` version
- `packages: ok`
- `python_yaml: ok` (the description check is `scripts/ci/check_model_descriptions.py`)
- `yq: ok` (the test-coverage check is `scripts/ci/check_model_tests.sh`)
- `ci_scripts: ok`
- `default_target: dev`
- `result: ready-static` or `result: ready-warehouse`

`ready-static` is enough for `features/static-pr-checks.md` and `features/model-ownership.md`. `ready-warehouse` requires `SNOWFLAKE_PAT` or `SNOWFLAKE_PRIVATE_KEY_PATH` so compile, build, test and `dbt ls` will not hang on SSO or password MFA. If doctor says `warehouse_noninteractive: no`, skip warehouse features and report the unmet precondition. Do not run `dbt debug` on an interactive authenticator.

Never print `SNOWFLAKE_*` values, key material, or `.env` contents.

Snowflake DEV is shared. Do not create task-specific databases, schemas or target prefixes. Do not assume exclusive use of DEV relations. Refuse `--target prod`, `--target snowflake-prod` and `--target ci-prod`.

## Drive

Harness: `control-dbt-analytics`. Stable handles are model names, `ref()` names, YAML `data_tests`, and the four CI scripts. Do not drive by warehouse object names.

```bash
H=./.cursor/skills/verify-dbt-analytics/bin/control-dbt-analytics

# Static PR checks (no warehouse). Pass changed SQL, or a known-good fixture when proving the checkers.
$H ci-static --files models/reference/data_dictionary/nhs_ethnicity_2001.sql
$H ci-static --base origin/main

# Ownership workflow (new models vs base only).
$H capture -- python3 scripts/ownership/check_model_ownership.py \
  --author-name verify --base-branch origin/main --output /tmp/ownership-suggestions.json

# Warehouse commands. Doctor must report ready-warehouse. Default target is dev.
$H compile -- -s nhs_ethnicity_2001
$H ls -- -s nhs_ethnicity_2001+
$H build -- -s nhs_ethnicity_2001
$H test -- -s nhs_ethnicity_2001
```

The helper refuses `dbt show` and production targets. Follow `PROJECT_CONVENTIONS.md`: smallest useful selection, then downstream only when consumers may change. `dbt show` is not a verification path here.

Exact GitHub Actions commands (from `.github/workflows/dbt-code-quality.yml`):

```bash
bash scripts/ci/check_hardcoded_refs.sh <sql files>
bash scripts/ci/check_staging_refs.sh <sql files>
python3 scripts/ci/check_model_descriptions.py <sql files>
bash scripts/ci/check_model_tests.sh <sql files>
```

Fusion compile in CI is `dbt compile --target ci-dev --profiles-dir .` on pull requests and `ci-prod` on merge-queue. Local verification uses `--target dev`. Do not use `ci-prod` from this harness.

## Evidence

Proof lives in `$VERIFY_DBT_EVIDENCE_DIR` if set, otherwise `/opt/cursor/artifacts/verify-dbt-analytics/$VERIFY_DBT_RUN_ID/` when that artifacts directory exists, otherwise `/tmp/verify-dbt-analytics-evidence/$VERIFY_DBT_RUN_ID/`. Cleanup must not delete this directory.

Each captured command writes `<name>.command.txt`, `<name>.stdout.log`, `<name>.stderr.log` and `<name>.exit_code.txt`. `ci-static` also writes `ci-static.files.txt` and `ci-static.summary.txt`.

Proof standards:

- Exercise the real user path: Fusion CLI and the CI scripts, not a private test endpoint and not `dbt show`.
- Capture the command and the resulting exit code plus redacted stdout/stderr, not only a final "passed" sentence.
- Static-check proof includes the file list and each checker’s `PASSED:` or `FAILED:` line.
- Compile proof includes Fusion success for the named selection, not compiled SQL dumps.
- Build/test proof includes node status counts only. Never copy model rows, `target/compiled` SQL that was executed as a preview, failing-test row dumps, or screenshots of warehouse data.
- High-level aggregates are allowed when they cannot identify anyone. Do not print suspected sensitive values.
- Record the feature ID and entry point with every artifact.
- An unreachable warehouse path is a skip with the unmet doctor line, not a pass via static checks.

## Cleanup

```bash
.cursor/skills/verify-dbt-analytics/bin/control-dbt-analytics cleanup
```

Removes `$VERIFY_DBT_SCRATCH_DIR` (default `/tmp/verify-dbt-analytics-$VERIFY_DBT_RUN_ID`). Does not kill processes by name. Does not drop Snowflake objects. Does not delete evidence. After cleanup, the evidence directory must still exist and still contain the captured files.

## Helpers

`bin/control-dbt-analytics` is executable. Invoke it from the repository root as shown above. Commands: `launch`, `doctor`, `ci-static`, `compile`, `build`, `test`, `ls`, `capture`, `cleanup`.
