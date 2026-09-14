{{ config(materialized='view') }}

/* Thin input for source_content_freshness_history. */

select
    source_schema,
    consensus_content_date as content_date
from {{ ref('source_content_freshness') }}
