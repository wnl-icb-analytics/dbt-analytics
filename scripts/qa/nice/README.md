# NICE regression checks

The programme classification, practice summaries, status enrichment and archive migration have
local unit tests with synthetic inputs. They do not connect to Snowflake:

```powershell
uv run --with duckdb --with jinja2 --with sqlglot python -m unittest discover -s scripts/qa/nice/tests -v
```

Clinical boundaries, record ordering and month-end rules for the NICE measures are covered by dbt
tests: grain and accepted-value tests in the model YAML, native unit tests, and synthetic singular
tests in `tests/nice*.sql` that call the calculation macros. They run when their models are selected.

After compiling dbt, check catalogue metadata and whether every individual indicator feeds the
final status table:

```powershell
python scripts/qa/nice/check_metadata.py target/manifest.json
```

This checks IDs, required catalogue properties, source flags, NICE usage, source links and model
lineage. The source review checks their clinical meaning.

Direct extraction and input rules formerly checked by the six DuckDB validators are covered by
native tests in `models/modelling/olids/{observations,person_attributes}/nice_extractor_unit.yml`,
alongside the existing CKD and childhood profile tests. These mock upstream inputs and execute
the tested model, including eGFR reporting-day timestamps and value-free tests, smoking-support
sources, vaccine code membership, alcohol screening and depression episode rules.
`tests/nice_s4_smoking_pharmacotherapy_refset_available.sql` retains the active smoking drug-refset
availability check. Tests of an indicator with mocked extractor output do not cover extraction.

## Programme validation in DEV

Validated on 6 October 2026 after merging main (`1bbb5951`) and the classification fix
(`1200df4c`). The `dev` build with `target_programme` passed for 12 models, one seed and
82 data tests. The metadata checker and six local synthetic tests also passed.

All 339 catalogue indicators have a programme, domain and subdomain. Counts cover
21 domains and 67 subdomains; every `IND%` entry is NICE. Current achievement totals
and five month-ends (October 2021, March 2023, March 2025, March 2026 and September 2026)
match the status tables per indicator and date. Row counts and aggregate hashes of
every existing status column are unchanged. Catalogue history retained its earlier
versions and gained `programme` and `clinical_subdomain`.

The fresh main manifest selects the expected 13 models and seeds for
`state:modified+`, including both LTC LCS dashboard bases. It selects no register,
vaccination calculation, LTC LCS calculation or individual NICE measure model.

All 245 singular tests ran: 238 passed, 5 failed and 2 warned. These five failures and two
warnings reproduce on merged main `e2fed7b6`, with identical failing-result
counts and unchanged test SQL and direct parent models:

- `apc_discharge_estimate_and_pod_rules` (fail).
- `epd_covers_slam_cost_window` (warn).
- `ltc_register_fct_pit_reconciliation` (warn).
- `ndh_ltc_summary_matches_register` (fail).
- `ndh_register_excludes_gdm_only` (fail).
- `nice_history_population_scope` (fail).
- `organisation_source_codes_retained` (fail).
