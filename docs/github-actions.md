# GitHub Actions

GitHub Actions provides static checks, compilation, merge-queue validation,
production deployment and project-board updates.

## Pull request checks

Every pull request receives fast feedback:

| Workflow | What it does |
|----------|--------------|
| `auto-author-assign.yml` | Assigns the pull request author |
| `dbt-code-quality.yml` | Checks hardcoded relations, raw/source layer boundaries, model descriptions and test coverage |
| `dbt-compile.yml` | Runs Fusion compile against development metadata |
| `model-ownership.yml` | Comments when changed models lack ownership metadata |
| `dbt-pr-validation.yml` | Reports that runtime validation will run in the merge queue |

The staging-reference check enforces the raw boundary in changed models. Staging
may use `ref()` to a generated raw model but not `source()`; every other
hand-written layer may use neither. Unrelated legacy models remain outside the
pull request.

CodeRabbit also reviews draft and ready pull requests against
`.coderabbit.yaml` and `PROJECT_CONVENTIONS.md`. It comments but does not
submit a formal request-changes review. Human reviewers decide whether findings
are resolved and must not merge a pull request with an unresolved data-safety
finding.

## Merge-queue validation

Selecting **Merge when ready** creates a merge-queue candidate. Required checks
then validate the exact commit GitHub would merge:

- `dbt-compile.yml` compiles against production metadata.
- `dbt-pr-validation.yml` compiles against production metadata, fetches the last
  deployed manifest and builds `state:modified` nodes in Snowflake development.
- Unselected parents defer to production relations.
- When no deployed manifest exists, validation builds the directly changed dbt
  nodes rather than the full project.

Merge-queue runtime builds are serial because candidates share development
relations. They do not publish deployment state.

## Production deployment

`dbt-deploy.yml` runs after dbt changes merge to `main`, or on manual dispatch.
It:

1. compiles the project with Fusion;
2. fetches the last deployed manifest;
3. builds `state:modified+` in production, including downstream consumers;
4. falls back to a full build for a manual run or when no state exists;
5. publishes dbt artifacts as the next deployment baseline; and
6. reports failures through a GitHub issue.

A commit or pull-request title containing `[skip deploy]` or `[skip-deploy]`
skips the automatic production deployment. Scheduled runs do not advance the
deployment-state manifest.

## Other workflows

| Workflow | Trigger and purpose |
|----------|---------------------|
| `dbt-scheduled.yml` | Runs configured dbt selections on a schedule or manual dispatch |
| `test-coverage.yml` | Updates the model test-coverage badge after model changes on `main` |
| `project-status-in-progress.yml` | Moves referenced issues to In Progress after branch pushes |
| `project-status-blocked.yml` | Moves issues labelled Blocked to Blocked |
| `project-status-review.yml` | Moves ready pull requests with reviewers to Code Review |

## Credentials

Runtime dbt workflows use the Snowflake service account through repository
secrets:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE__USERNAME`
- `SNOWFLAKE__PRIVATE_KEY`
- `SNOWFLAKE__PASSPHRASE`

Project-board automation uses `PROJECT_TOKEN`; the coverage badge uses
`GIST_TOKEN`. Workflows write private keys only for the current job and remove
them in an `always()` cleanup step.
