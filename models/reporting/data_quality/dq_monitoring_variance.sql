{{ config(materialized='view') }}

select
    measurement_date,
    measured_at,
    table_id,
    row_count,
    refresh_date,
    days_since_refresh,
    frequency_type,

    lag(row_count) over (
        partition by table_id
        order by measurement_date
    ) as previous_row_count,

    row_count
        - lag(row_count) over (
            partition by table_id
            order by measurement_date
        ) as row_count_change,

    round(
        (
            (row_count
                - lag(row_count) over (
                    partition by table_id
                    order by measurement_date
                )
            )::float
            /
            nullif(
                lag(row_count) over (
                    partition by table_id
                    order by measurement_date
                ),
                0
            )
        ) * 100,
        2
    ) as row_count_change_pct

from {{ ref('dq_monitoring_metrics_history') }}
