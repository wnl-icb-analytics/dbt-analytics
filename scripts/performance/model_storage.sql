-- Operational metadata only. Join this export to manifest lineage with audit_model_reuse.py.
select
    table_catalog,
    table_schema,
    table_name,
    row_count,
    bytes,
    clustering_key
from snowflake.account_usage.tables
where deleted is null
    and table_type = 'BASE TABLE'
    and table_catalog in ('STAGING', 'MODELLING', 'REPORTING', 'REFERENCE')
    and row_count >= 1000000
order by bytes desc
