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
