{{
    config(
        description="Raw layer: Clinical code-set membership across maintained sources. Rebuilt by a Snowflake task, not dbt; staging provides one row per cluster, code and source. 1:1 passthrough with cleaned column names. \nSource: REFERENCE.TERMINOLOGY.COMBINED_CODESETS \ndbt: source(''reference_terminology_curated'', ''combined_codesets'') \nColumns:\n  CLUSTER_ID -> cluster_id\n  CLUSTER_DESCRIPTION -> cluster_description\n  CODE -> code\n  CODE_DESCRIPTION -> code_description\n  SOURCE -> source"
    )
}}
select
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "CODE" as code,
    "CODE_DESCRIPTION" as code_description,
    "SOURCE" as source
from {{ source('reference_terminology_curated', 'combined_codesets') }}
