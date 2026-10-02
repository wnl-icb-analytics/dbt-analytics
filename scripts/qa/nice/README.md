# NICE regression checks

Clinical boundaries, record ordering and month-end rules for the NICE measures are covered by dbt
tests: grain and accepted-value tests in the model YAML, native unit tests, and singular tests in
`tests/nice_history_*.sql` that call each calculation macro with synthetic inputs. They run with
the models in every build.

After compiling dbt, check catalogue metadata and whether every individual indicator feeds the
final status table:

```powershell
python scripts/qa/nice/check_metadata.py target/manifest.json
```

This checks IDs, required catalogue properties, source flags, NICE usage, source links and model
lineage. The source review checks their clinical meaning.
