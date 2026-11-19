{{ config(materialized='view') }}

select

    -- Identity
    v.measurement_date,
    v.measured_at,
    v.table_id,

    -- Monitoring metrics
    v.row_count,
    v.previous_row_count,
    v.row_count_change,
    v.row_count_change_pct,

    v.refresh_date,
    v.days_since_refresh,
    v.frequency_type,

    -- Monitoring thresholds
    c.warn_stale_days,
    c.critical_stale_days,
    c.warn_row_change_pct,
    c.critical_row_change_pct,

    -- Classification
    c.business_area,
    c.object_type,

    -- Freshness status
    case

        when v.refresh_date is null
            then 'GREY'
        when v.days_since_refresh > c.critical_stale_days
            then 'RED'
        when v.days_since_refresh > c.warn_stale_days
            then 'AMBER'
        else 'GREEN'
    end as freshness_status,

case
    when
        (
            v.days_since_refresh > c.critical_stale_days
            or
            (
                v.previous_row_count is not null
                and abs(v.row_count_change_pct) > c.critical_row_change_pct
            )
        )
        then 1
    when
        (
            v.days_since_refresh > c.warn_stale_days
            or
            (
                v.previous_row_count is not null
                and abs(v.row_count_change_pct) > c.warn_row_change_pct
            )
        )
        then 2
    when
        (
            v.refresh_date is null
            or v.row_count is null
        )
        then 3
    else 4
end as status_rank,

    -- Variance status
    case
        -- First measurement has no comparison
        when v.row_count is null
            then 'GREY'
        when v.previous_row_count is null
            then 'GREEN'        
        when abs(v.row_count_change_pct) > c.critical_row_change_pct
            then 'RED'
        when abs(v.row_count_change_pct) > c.warn_row_change_pct
            then 'AMBER'
        else 'GREEN'
    end as variance_status,

    -- Overall status
    case

        when
            (
                v.days_since_refresh > c.critical_stale_days
                or
                (
                    v.previous_row_count is not null
                    and abs(v.row_count_change_pct) > c.critical_row_change_pct
                )
            )
            then 'RED'

        when
            (
                v.days_since_refresh > c.warn_stale_days
                or
                (
                    v.previous_row_count is not null
                    and abs(v.row_count_change_pct) > c.warn_row_change_pct
                )
            )
            then 'AMBER'

        else 'GREEN'

    end as overall_status,

    -- Human-readable explanation
    case

        when v.days_since_refresh > c.critical_stale_days
            then 'Refresh overdue - critical threshold exceeded'

        when v.days_since_refresh > c.warn_stale_days
            then 'Refresh overdue - warning threshold exceeded'

        when v.previous_row_count is not null
             and abs(v.row_count_change_pct) > c.critical_row_change_pct
            then 'Row count variance exceeded critical threshold'

        when v.previous_row_count is not null
             and abs(v.row_count_change_pct) > c.warn_row_change_pct
            then 'Row count variance exceeded warning threshold'

        else 'Monitoring metrics within expected thresholds'

    end as status_reason

from {{ ref('dq_monitoring_variance') }} v

inner join {{ ref('dq_monitoring_config') }} c
    on v.table_id = c.table_id

where c.is_active = true
