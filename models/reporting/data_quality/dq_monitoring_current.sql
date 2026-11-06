{{ config(materialized='view') }}

select *
from {{ ref('dq_monitoring_status') }}

qualify row_number()
over (
    partition by table_id
    order by measurement_date desc,
             measured_at desc
) = 1
