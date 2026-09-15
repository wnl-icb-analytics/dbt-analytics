# Build and test a model

Build and test a model lets an analyst materialise a named selection on the shared `dev` target and run that model's tests.

## Sub-features

- `build-named` runs `dbt build -s <model>` (run plus tests).
- `test-named` runs `dbt test -s <model>` when the relation already exists.
- `build-downstream` runs `dbt build -s <model>+` only when consumers may change.

## How to get to it (user POV)

- Run `dbt build -s model_name` as in README, CONTRIBUTING and `PROJECT_CONVENTIONS.md`.
- On Windows, run `.\build_changed.ps1` (optional `-u`, `-d`, `-r`, `-t`). On Linux, select the same models with `dbt build -s` and `git diff`.
- After **Merge when ready**, merge-queue `dbt-pr-validation.yml` builds `state:modified` in Snowflake DEV. That path is CI, not this harness.

## Driving it with control-dbt-analytics

Preconditions:

- Doctor reports `result: ready-warehouse`.
- Selection is the smallest that answers the question. DEV is shared.
- Tests that fail will try to return failing rows. Do not copy those rows into evidence, git or chat.

- **Named build.** Build the fixture if warehouse proof is in scope. Run `control-dbt-analytics build -- -s nhs_ethnicity_2001`. Exit code `0`. Stdout shows the model and its tests as success. Evidence records node names and status only.
- **Named tests.** Re-run tests without rebuilding. Run `control-dbt-analytics test -- -s nhs_ethnicity_2001`. Exit code `0`. Same evidence rule.
- **Downstream limit.** List consumers first (`features/downstream-impact.md`). Only then `control-dbt-analytics build -- -s nhs_ethnicity_2001+` if that set is small enough. If it is not, skip and say so.
- **Proof.** Keep `dbt-build.exit_code.txt` (or `dbt-test`) at `0` and a redacted log with success counts, not `select` results.

## Gotchas

- DEV is not isolated. Building a popular model overwrites the shared relation. Keep the selection small and avoid repeating full `+` trees.
- `build_changed.ps1` is PowerShell. Do not call it from this Linux harness.
- Merge-queue validation uses `--defer --favor-state` and a deployed manifest. A local `dbt build -s` is not that dress rehearsal.
- Failing tests can emit person-level rows. Treat any returned test row as potentially identifying. Record only that the test failed and the test name.
- `store_failures` is false in `dbt_project.yml`. Do not turn it on for verification.
- Never `--target prod`.
