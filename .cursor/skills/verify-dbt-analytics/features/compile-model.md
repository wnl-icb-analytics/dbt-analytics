# Compile a model

Compile a model lets an analyst run Fusion static analysis on a named selection and learn whether refs, Jinja and SQL parse against development metadata without materialising tables.

## Sub-features

- `compile-named` compiles one model with `dbt compile -s <model>`.
- `compile-plus-upstream` compiles a model with its parents using `dbt compile -s +<model>`.
- `compile-ci-dev` is the pull-request compile gate: `dbt compile --target ci-dev --profiles-dir .` (full project, needs non-interactive warehouse auth).

## How to get to it (user POV)

- Run `dbt compile -s model_name` as in README and CONTRIBUTING.
- Push a pull request and wait for `dbt-compile.yml` (`Fusion compile` against `ci-dev`).
- Run `control-dbt-analytics compile -- -s <model>`.

## Driving it with control-dbt-analytics

Preconditions:

- Doctor reports `result: ready-warehouse`.
- Fusion packages are installed.
- The default target remains `dev`. Do not pass `prod`, `snowflake-prod` or `ci-prod`.

- **Named model.** Compile the reference fixture. Run `control-dbt-analytics compile -- -s nhs_ethnicity_2001`. Exit code `0`. `dbt-compile.stdout.log` shows Fusion completed the selection. Evidence does not include executed preview rows.
- **Upstream selection.** Compile with parents. Run `control-dbt-analytics compile -- -s +nhs_ethnicity_2001`. Exit code `0`. The log names the compiled nodes without warehouse row content.
- **Proof.** Keep `dbt-compile.command.txt`, `dbt-compile.exit_code.txt` and the redacted stdout. The command contains `-s nhs_ethnicity_2001` and the exit code is `0`.

## Gotchas

- Fusion compile reads Snowflake catalog metadata. Password or SSO auth can hang in a cloud agent. Skip when `warehouse_noninteractive: no`.
- `ci-prod` is the merge-queue compile target. This harness refuses it.
- Compiling the whole project is slow and is what CI does, not a local proof of one model change.
- Do not use `dbt show` to "see if compile worked".
- Compiled SQL under `target/` is working output, not proof to paste into a public pull request.
