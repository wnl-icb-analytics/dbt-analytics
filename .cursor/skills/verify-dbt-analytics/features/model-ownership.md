# Model ownership

Model ownership lets an analyst see whether newly added models declare `config.meta.owner.name` before the `model-ownership.yml` workflow comments on the pull request.

## Sub-features

- `ownership-clean` reports zero suggestions when no new model SQL is missing an owner.
- `ownership-new` writes JSON suggestions for new `models/**/*.sql` files (except raw) that lack owner metadata.
- `ownership-skip-existing` ignores models that already have `config.meta.owner`.

## How to get to it (user POV)

- Add a model and open a pull request; `model-ownership.yml` comments when owner metadata is missing.
- Run `python3 scripts/ownership/check_model_ownership.py` locally with an author name and base branch.

## Driving it with control-dbt-analytics

Preconditions:

- Doctor reports `result: ready-static`.
- Compare against `origin/main` unless the pull request targets another branch.
- The output JSON path must sit in scratch or evidence, not in `models/`.

- **Branch with no new models.** Run `control-dbt-analytics capture -- python3 scripts/ownership/check_model_ownership.py --author-name verify --base-branch origin/main --output "$VERIFY_DBT_SCRATCH_DIR/suggestions.json"`. Exit code `0`. Stdout contains `Generated 0 suggestions` (or a positive count only when new models on the branch lack owners). The JSON is `[]` when clean.
- **Proof.** Keep `capture.stdout.log`, `capture.exit_code.txt` and a copy of `suggestions.json` in the evidence directory. The JSON has no credentials and no patient data.

## Gotchas

- The script only sees `--diff-filter=A` (added) SQL under `models/`. Edits to existing models will not appear, even if owner metadata is missing.
- Raw models are excluded.
- Owner must be the business stakeholder for the model's meaning, not the git author merely because they opened the pull request. The workflow uses the PR author as a starting suggestion only.
- `uv run --with pyyaml` is what CI uses. `python3` with PyYAML already imported is enough here.
