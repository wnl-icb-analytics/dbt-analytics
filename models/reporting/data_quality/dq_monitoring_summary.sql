{{ config(materialized='view') }}

select
    overall_status,
    count(*) as table_count,

    min(status_rank) as status_rank

from {{ ref('dq_monitoring_current') }}

group by overall_status
