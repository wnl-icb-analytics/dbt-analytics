# dbt-analytics verification map

This directory is the maintained source for verifying dbt-analytics from the Fusion CLI and the pull-request static scripts. Read this index before driving, then use the matching feature file as the recipe.

## Baseline preconditions

- Work from the git repository root. Put `~/.local/bin` on `PATH`.
- Run `.cursor/skills/verify-dbt-analytics/bin/control-dbt-analytics launch`.
- Run `control-dbt-analytics doctor` and require `fusion: ok`, `packages: ok`, `python_yaml: ok`, `yq: ok` and `ci_scripts: ok`.
- Static features need `result: ready-static`. Warehouse features need `result: ready-warehouse`.
- Use the established `dev` target and existing `DEV__` layers. Do not create extra databases or schema prefixes.
- Never drive `--target prod`, `--target snowflake-prod` or `--target ci-prod`.
- Never run `dbt show` in this harness. Never print `SNOWFLAKE_*` values or patient-level rows.

## Driving conventions

- Start every recipe from the baseline unless its preconditions say otherwise.
- Treat every command as literal. Keep model names, flags and script paths unchanged.
- Prefer `control-dbt-analytics` over calling Fusion in a way that skips the production-target and `dbt show` guards.
- Pass SQL paths relative to the repository root, matching GitHub Actions.
- Restore nothing in Snowflake after a selected `dev` build; DEV is shared. Keep selections small.
- Do not remove proof artifacts during cleanup.

## Proof and skip reporting

- Capture the command, redacted stdout/stderr and exit code, not only the last line.
- Static proof includes the file list and each `PASSED:` / `FAILED:` line.
- Warehouse proof includes node counts or compile success for the named selection, never row dumps.
- Record the feature ID and entry point used with every artifact.
- Report an unreachable path with the attempted command and the unmet doctor line.
- Do not report a skipped warehouse entry point as verified through a static check.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph describing the user-visible behaviour. It then uses exactly four H2 sections in this order.

1. `Sub-features` lists short IDs with one line for each behaviour.
2. `How to get to it (user POV)` lists every user entry point.
3. `Driving it with control-dbt-analytics` starts with `Preconditions:` and uses labelled bullets that pair each user action with an exact command and observable result.
4. `Gotchas` lists traps that can waste or invalidate a verification run.

Keep implementation details out of the map. Name only user paths, stable handles, required state, commands and observable proof.

## Features

- [Static PR checks](./static-pr-checks.md) covers hardcoded relations, raw/source boundaries, model descriptions and declared tests.
- [Compile a model](./compile-model.md) covers Fusion compile of a named selection against development metadata.
- [Build and test a model](./build-and-test-model.md) covers `dbt build` and `dbt test` for a named selection on the `dev` target.
- [Downstream impact](./downstream-impact.md) covers `dbt ls -s model_name+` before a change.
- [Model ownership](./model-ownership.md) covers the ownership metadata check for new models.
