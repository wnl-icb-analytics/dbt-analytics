{{ config(materialized='view') }}

select
    measurement_date,
    measured_at,
    table_id,
    provider_code,
    provider_name,
    row_count,
    latest_refresh_date,
    latest_actual_financial_year,
    latest_actual_financial_month,
    latest_update_date,

    lag(row_count) over (
        partition by
            table_id,
            provider_code
        order by
            measurement_date
    ) as previous_row_count,

    row_count
        - lag(row_count) over (
            partition by
                table_id,
                provider_code
            order by
                measurement_date
        ) as row_count_change,

    round(
        (
            (
                row_count
                - lag(row_count) over (
                    partition by
                        table_id,
                        provider_code
                    order by
                        measurement_date
                )
            )::float
            /
            nullif(
                lag(row_count) over (
                    partition by
                        table_id,
                        provider_code
                    order by
                        measurement_date
                ),
                0
            )
        ) * 100,
        2
    ) as row_count_change_pct

from {{ ref('dq_monitoring_provider_history') }}
