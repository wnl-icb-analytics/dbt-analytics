# NICE regression checks

These checks execute repository SQL against synthetic fixtures in DuckDB. They
cover clinical boundaries and record ordering in the NICE models. They do not
connect to Snowflake or read patient data.

Run each `validate_*.py` script with its local dependencies:

```powershell
Get-ChildItem scripts/qa/nice/validate_*.py | ForEach-Object {
    uv run --no-project --with duckdb --with jinja2 --with pandas --with pyyaml --with sqlglot python $_.FullName
    if ($LASTEXITCODE -ne 0) { throw "NICE regression failed" }
}
```

The scripts render the actual model SQL and replace Snowflake-specific date and
aggregate functions for DuckDB. They use a fixed reporting date, with extra dates
for financial-year boundaries. A passing result does not replace the project’s
Snowflake compile, build and downstream tests.

After compiling dbt, check metadata and whether every individual indicator feeds
the final status table:

```powershell
python scripts/qa/nice/check_metadata.py target/manifest.json
```

This checks IDs, required catalogue properties, source flags, NICE usage, source
links and model lineage. The source review checks their clinical meaning.
