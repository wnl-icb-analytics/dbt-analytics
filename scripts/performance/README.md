# Performance checks

## Shared models and repeated view work

Export `scripts/performance/model_storage.sql` with the Snowflake CLI using
`DBT_ADMIN` and JSON output. Then combine it with a current dbt manifest:

```powershell
snow sql -f scripts/performance/model_storage.sql -c dbt-admin --format json > tmp/model-storage.json
python scripts/performance/audit_model_reuse.py --tables tmp/model-storage.json > tmp/model-reuse.json
```

The report lists direct model consumers, table size, configured and physical
clustering keys, and staging views containing possible repeated transformations.
It reads metadata only. Without `--tables`, it still lists shared staging views.
SQL keyword matches are a shortlist, not proof of expensive execution. They can
include comments and miss operations hidden in macros. Account Usage can lag
recent builds, and consumer counts do not measure scan frequency or cost.

A view reads the underlying table's micro-partitions and can benefit from its
clustering. In particular, dbt-olids already clusters the observation and
medication inputs. Materialise a view only when saved repeated transformation
work outweighs the extra build and storage, and its refresh schedule preserves
the required freshness. For large shared outputs, inspect actual downstream
filters and joins before choosing `cluster_by`. Compare pruning and downstream
execution savings with the extra sort and clustering maintenance cost.

## EPD incremental builds

Use the tracked `dev` target and existing `DEV__` layers for validation. These
relations are shared with other development and merge-validation builds.
Do not create task-specific schemas or override the project's naming macros.

```powershell
dbt build -s stg_epd_pc_meds+ --target dev
```

EPD replaces complete changed months. Its change check compares row counts and
`HASH_AGG` values over the projected source and stored output. This still scans
both datasets, but avoids rewriting unchanged months. It does not assume that
submission IDs or row counts alone detect corrections. Hash comparison is
probabilistic, and the existing monthly full refresh remains the complete
reconciliation.

An incremental build stops if a changed month has disappeared entirely or has a
null period. Reconcile the source with:

```powershell
dbt build -s stg_epd_pc_meds --full-refresh --target dev
```

For production, use the corresponding approved production build. Production
tags and selections are unchanged. EPD remains eligible for daily and weekly
builds, and the monthly workflow still supplies `--full-refresh`.

When measuring EPD, include the model's content-comparison `SELECT` as well as
its CTAS and incremental DML statements. The empty write on an unchanged run
does not represent its total cost. The comparison carries the model, target and
invocation in a SQL comment, with `operation: compare_processing_periods`.
Compare the same warehouse size and source
volume, and separate execution from queueing.
