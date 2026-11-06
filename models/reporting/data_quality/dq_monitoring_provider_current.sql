{{ config(materialized='view') }}

select *
from {{ ref('dq_monitoring_provider_status') }}

qualify row_number() over (
    partition by
        table_id,
        provider_code

    order by
        measurement_date desc,
        measured_at desc nulls last
) = 1
