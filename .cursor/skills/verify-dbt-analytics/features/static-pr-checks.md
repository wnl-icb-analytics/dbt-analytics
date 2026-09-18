# Static PR checks

Static PR checks let an analyst run the four `dbt-code-quality.yml` scripts on model SQL and see each check pass or fail before GitHub Actions does.

## Sub-features

- `static-hardcoded` rejects three-part `FROM`/`JOIN` identifiers in changed SQL.
- `static-staging` rejects `source()` outside generated raw models and `ref('raw_…')` outside staging.
- `static-descriptions` requires a non-empty model description in YAML or a SQL config block.
- `static-tests` requires at least one YAML test on each non-raw, non-semantic model.
- `static-empty` reports `PASSED: No files to check.` when the file list is empty (not proof of the checkers).

## How to get to it (user POV)

- Change a model and wait for the `dbt Code Quality` workflow on the pull request.
- Run the four scripts locally on the changed SQL paths, as `.github/workflows/dbt-code-quality.yml` does.
- Run `control-dbt-analytics ci-static` with `--files` or with `--base origin/main`.

## Driving it with control-dbt-analytics

Preconditions:

- Doctor reports `result: ready-static` (warehouse is not required).
- `yq` and `python3` with PyYAML are available.
- For a proof of the checkers themselves, do not rely on an empty git diff.

- **Known-good fixture.** Check a documented reference model that already has description, tests and `ref()`. Run `control-dbt-analytics ci-static --files models/reference/data_dictionary/nhs_ethnicity_2001.sql`. Exit code `0`. `ci-static.files.txt` contains that path. Each of `ci-hardcoded`, `ci-staging`, `ci-descriptions` and `ci-tests` stdout ends with a `PASSED:` line and exit code `0`.
- **Changed-files entry.** Check the branch against main. Run `control-dbt-analytics ci-static --base origin/main`. If SQL files changed, the same four `PASSED:` or `FAILED:` lines appear for those paths. If no SQL files changed, stdout says the run is a skip and is not proof of the checkers.
- **Hardcoded-refs script.** Run the Actions command. Run `bash scripts/ci/check_hardcoded_refs.sh models/reference/data_dictionary/nhs_ethnicity_2001.sql`. Exit code `0` and stdout `PASSED: No hardcoded table references found.`
- **Staging-refs script.** Run `bash scripts/ci/check_staging_refs.sh models/reference/data_dictionary/nhs_ethnicity_2001.sql`. Exit code `0` and stdout `PASSED: All raw/source references follow the layer boundary.`
- **Descriptions script.** Run `python3 scripts/ci/check_model_descriptions.py models/reference/data_dictionary/nhs_ethnicity_2001.sql`. Exit code `0` and stdout `PASSED: All models have descriptions.`
- **Tests script.** Run `bash scripts/ci/check_model_tests.sh models/reference/data_dictionary/nhs_ethnicity_2001.sql`. Exit code `0` and stdout `PASSED: All models have tests.`
- **Proof.** Keep `ci-static.summary.txt` plus the four `ci-*.stdout.log` files. The summary `result:` line is `PASSED` and lists the fixture path.

## Gotchas

- Passing no files is a skip. GitHub Actions also skips the job when the changed-file list is empty. Do not treat that as a verified checker.
- Description CI uses the Python script, not `scripts/ci/check_model_descriptions.sh`.
- Raw models are exempt from the tests check. Semantic models are exempt from the tests check. Staging may `ref()` `raw_*` models; other layers may not.
- The hardcoded-refs pattern looks for three-part identifiers after `FROM`/`JOIN`. Information schema names are ignored.
- `yq` missing fails `check_model_tests.sh` before any YAML is read.
- Do not paste SQL that contains credentials or person-level values into evidence.
