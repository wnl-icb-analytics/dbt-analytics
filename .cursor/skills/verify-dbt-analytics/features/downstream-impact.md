# Downstream impact

Downstream impact lets an analyst list which models would rebuild if a named model changed, before they edit SQL.

## Sub-features

- `ls-self` lists the named model with `dbt ls -s <model>`.
- `ls-downstream` lists the model and its consumers with `dbt ls -s <model>+`.
- `ls-empty-miss` reports no nodes when the name is wrong.

## How to get to it (user POV)

- Run `dbt ls -s model_name+` as in README, CONTRIBUTING and `PROJECT_CONVENTIONS.md`.
- Run `control-dbt-analytics ls -- -s <model>+`.

## Driving it with control-dbt-analytics

Preconditions:

- Doctor reports `result: ready-warehouse`. Fusion `ls` still talks to the project and may need catalog metadata.
- Use a real model name. The fixture is `nhs_ethnicity_2001`.

- **Self list.** Run `control-dbt-analytics ls -- -s nhs_ethnicity_2001`. Exit code `0`. Stdout contains `nhs_ethnicity_2001` and does not contain person-level data.
- **Downstream list.** Run `control-dbt-analytics ls -- -s nhs_ethnicity_2001+`. Exit code `0`. Stdout includes the fixture and any consumer model names.
- **Proof.** Keep `dbt-ls.stdout.log` and `dbt-ls.exit_code.txt`. The log is a name list, not a query result.

## Gotchas

- Skip when `warehouse_noninteractive: no`. Do not hang on SSO.
- `dbt ls` output is model names. That is safe to keep. Do not follow it with `dbt show`.
- A large `+` tree is expected for core person models. Do not build the whole tree as part of listing it.
- Resource type filters (`--resource-type model`) keep tests out of the list when you only want models.
