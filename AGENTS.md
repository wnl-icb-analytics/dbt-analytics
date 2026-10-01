# Working in dbt-analytics

This is a public NHS dbt project on Snowflake. Read
`PROJECT_CONVENTIONS.md` before model work.

Apply these writing rules to responses, documentation, comments, commit messages
and pull requests:

- Use plain British English, concrete facts and active voice. Say what changed,
  why it matters and who or what acts.
- Prefer a common word to jargon. Explain unfamiliar project terms when the
  context does not make them clear.
- In this project, `model contract` has the meaning defined in
  `PROJECT_CONVENTIONS.md`. Do not use it as a general term for a model, grain,
  rule, output, requirement or design decision.
- Cut filler, puffery, vague claims and stock AI phrasing. Remove sentences that
  could appear unchanged in any project's documentation.
- When judgement is needed, state your view and the real trade-off. Use first
  person when it sounds natural. Do not flatten complexity into generic pros
  and cons.
- Prefer named sources and concrete examples to vague attribution. Say who made
  a claim or which report contains it, and explain why it matters.
- Remove chatbot filler, generic openings and generic conclusions. Cut
  excessive hedging and closing invitations that add no information.
- Avoid stock AI vocabulary and abstract technical metaphors when a concrete
  project term exists. Say `model`, `field`, `rule`, `move` or `delete` when
  that is what you mean.
- Keep one main idea per sentence, but vary sentence length and rhythm. Do not
  force points into groups of three or cycle through synonyms for a clear term.
- Use sentence case headings, straight quotes and restrained formatting. Avoid
  em dashes, decorative emoji and unnecessary bold text.
- Preserve code, quoted text, technical syntax and accuracy when applying these
  rules.
- Before sending prose, ask which lines sound generated or could belong to any
  project. Rewrite or remove them.

Inspect the intended outcome, real constraint, related models, shared
definitions, configuration and lineage before editing. Prefer the smallest
coherent design, not the smallest diff or narrowest answer. Reuse established
work, but do not preserve complexity merely because it exists.

Many healthcare objects in the warehouse are not yet represented in dbt. For a
new domain, model generally useful, durable entities or concepts at an explicit
grain. Include fields and shared definitions with plausible analytical use; do
not bury shared domain logic in a report-specific pipeline or copy every source
column. If this would materially widen the request, explain the opportunity and
agree the boundary with the user.

Use these principles when reasoning about every change:

- Measure twice, cut once: inspect existing models, shared definitions, lineage
  and consequences first.
- YAGNI: avoid machinery for hypothetical needs, but not reusable domain
  modelling.
- KISS and simple design: prefer readable SQL and clear model responsibilities.
- DRY and the Rule of Three: centralise stable business definitions; abstract
  implementation patterns only after they repeat and prove stable.
- Make it work, make it right, make it fast: before merge, demonstrate correct
  results, clear design and performance that fits the expected scale. Do not
  turn this into speculative tuning.

Raise a concern before implementing a direction likely to cause wrong results,
an unclear requirement, a duplicate pipeline or avoidable cost. State the
consequence and offer the smallest realistic alternative without turning it
into an unrequested redesign. Make routine, reversible choices yourself. Ask
the user who owns or authorises a clinical or business definition when that is
unclear, and before changing scope, the meaning or output of an analyst-facing
model, or anything hard to reverse. When both choices are safe and follow
project rules, let the user decide. Public-data safety and the raw-to-staging
boundary are hard constraints.

Before writing SQL:

- State the subject and grain. Add population and time when the model selects or
  derives them.
- Search model names, YAML and lineage. Start from the most downstream
  established model whose subject, grain, population and outputs fit the work.
  Move upstream only when that model is insufficient. Reuse, compose or extend
  where possible. Create a model or seed only for a distinct, durable purpose.
- Only staging models may reference `raw_` models. Hand-written models use
  `ref()`.
- Do not guess clinical meaning. Make population, code-list, threshold and date
  rules visible, and ask when their authority or interpretation is unclear.
- Use SQL comments for non-obvious meaning, source quirks or surprising choices,
  not to narrate SQL. Update or remove them when the logic changes.
- Treat reporting and published models as analyst interfaces. Choose grain and
  columns deliberately; remove fields known to be duplicate, unused or mostly
  empty when they have no analytical value. Use clear names and pair opaque
  codes with authoritative labels. A modelling block may remain code-only when
  the downstream interface supplies the labels.

Project configuration is part of the model. Folder placement supplies database,
schema, materialisation, tags and hooks through `dbt_project.yml`; check nearby
models before adding a local override. Call out `dbt_project.yml` changes because
they can affect many models. Tags drive schedules; hooks apply grants, comments
and governance.

Write YAML descriptions for analysts. State the subject, what one row represents
and the population scope. Explain material inclusion or exclusion rules,
thresholds, dates and definitions without narrating the SQL.
Project hooks publish model descriptions and `persist_docs` publishes column
descriptions to Snowflake metadata used by Snowsight and other tools. Document
units, codes and null meaning where they affect interpretation.

Keep SQL, descriptions, business ownership and tests together. Test every
model's grain with its key or key combination. This project does not use
test-driven development. Beyond grain, add permanent tests only for documented
rules or errors likely to recur. Tests run on every build and consume Snowflake
compute; do not test implementation details or repeat upstream assertions.

Check downstream impact with `dbt ls -s model_name+`. Compile and build the
smallest useful selection, then build downstream models whose results may
change. Use `dbt show` only under the data-safety rules below.

Use the established dbt `dev` target and database layers for development builds.
DEV is intentionally shared and not fully isolated. A Git worktree isolates
code, not warehouse objects. Never create task-specific databases, schemas or
target prefixes to isolate a build. Keep models alongside their existing domain
models and follow `PROJECT_CONVENTIONS.md` for build selection and dependencies.

Check the branch and worktree status, preserve unrelated work and read the diff
before pushing. Never work on `main`; use a `type/short-description` branch. Use
Conventional Commit form for commits and the pull request title. Open the
description with the problem and why it matters, then give the solution,
validation and review focus. Do not add attribution to an AI agent, language
model or execution harness.

Focus on what was corrected. The Conventional Commit scope is secondary:

- Weak title: `fix: referral dedup changes`
- Stronger title: `fix(mhsds): retain the latest version of each referral`
- Weak opening: `change the row_number ordering in the referral model`
- Stronger opening: `Resubmitted MHSDS referrals can have several versions. The
  current ordering can retain an older version, which leaves downstream reports
  with stale referral details.`

Draft pull requests are fine while work or decisions remain; CodeRabbit reviews
them. Before human review, fetch and check against `origin/main`, then surface
conflicts. Do not rebase or force-push a shared branch without the user's
approval.

When asked to monitor a pull request, review checks and comments posted after
the latest push. Verify automated findings against the diff and source. If asked
to resolve feedback, fix genuine issues and answer inaccurate findings with a
brief reason; do not change code merely to satisfy a bot.

Never include credentials or real patient- or person-level data in repository
files or GitHub text, including seeds, test data, query results, logs, errors,
screenshots and examples. Use synthetic data. High-level aggregates are allowed
when they cannot identify anyone. Do not repeat suspected sensitive values.
Alert repository maintainers because deleting the latest diff does not remove
public history.

Assume agent command output is visible to its provider. Use `dbt show` sparingly
and only for a query designed to return a high-level, non-identifying aggregate;
never use it to preview model rows. If validation needs row-level inspection,
give the user a Snowflake-native query for a human-controlled Snowflake session
approved for patient-level data. Ask only for the safe aggregate or confirmation
needed. Apply the same rule to ad hoc queries and failing-test SQL.
