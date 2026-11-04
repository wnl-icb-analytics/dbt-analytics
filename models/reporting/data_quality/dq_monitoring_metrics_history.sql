{{
    config(
        materialized='incremental',
        unique_key=['measurement_date','table_id']
    )
}}

/* Run daily to build data quality metrics history */

with measurements as (

    select
        current_date() as measurement_date,
        current_timestamp() as measured_at,
        'CSDS_REFERRAL' as table_id,
        count(*) as row_count,
        max(refresh_date) as refresh_date,
        datediff(
            day,
            max(refresh_date),
            current_date()
        ) as days_since_refresh,
        'MONTHLY' as frequency_type
    from REPORTING.MAIN_DATA.CSDS_REFERRAL

    union all

    select
        current_date() as measurement_date,
        current_timestamp() as measured_at,
        'CSDS_CARE_CONTACT' as table_id,
        count(*) as row_count,
        max(refresh_date) as refresh_date,
        datediff(
            day,
            max(refresh_date),
            current_date()
        ) as days_since_refresh,
        'MONTHLY' as frequency_type
    from REPORTING.MAIN_DATA.CSDS_CARE_CONTACT

    union all

    select
        current_date() as measurement_date,
        current_timestamp() as measured_at,
        'CSDS_CARE_CONTACT_ACTIVITY' as table_id,
        count(*) as row_count,
        max(created_date) as refresh_date,
        datediff(
            day,
            max(created_date),
            current_date()
        ) as days_since_refresh,
        'MONTHLY' as frequency_type
    from REPORTING.MAIN_DATA.CSDS_CARE_CONTACT_ACTIVITY

    union all

    select
        current_date() as measurement_date,
        current_timestamp() as measured_at,
        'SUS_IP' as table_id,
        count(*) as row_count,
        max(refresh_date) as refresh_date,
        datediff(
            day,
            max(refresh_date),
            current_date()
        ) as days_since_refresh,
        'DAILY' as frequency_type
    from REPORTING.MAIN_DATA.IP

    union all

    -- =====================================================
    -- SLAM Aggregate Contract Monitoring
    -- =====================================================

    select
        current_date() as measurement_date,
        current_timestamp() as measured_at,
        'SLAM_LSACM' as table_id,
        count(*) as row_count,
    max(
        case
            when DV_DATEANDTIMEDATASETCREATED <= current_timestamp() --- ignore future dates
            then to_date(DV_DATEANDTIMEDATASETCREATED)
        end
    ) as refresh_date,
    datediff(
        day,
        max(
            case
                when DV_DATEANDTIMEDATASETCREATED <= current_timestamp()
                then to_date(DV_DATEANDTIMEDATASETCREATED)
            end
        ),
            current_date()
        ) as days_since_refresh,
        'MONTHLY' as frequency_type

    from DATA_LAKE.SERVICES_DATA."LSACM_LocalStandardAggregateContractMonitoring"

union all

-- =====================================================
-- SLAM Patient Level Contract Monitoring
-- Ignoring invalid future dataset-created timestamps (eg. Moorfields 2024 file loads)
-- =====================================================

select
    current_date() as measurement_date,
    current_timestamp() as measured_at,
    'SLAM_LSPLCM' as table_id,
    count(*) as row_count,
    max(
        case
            when DV_DATEANDTIMEDATASETCREATED <= current_timestamp()
            then to_date(DV_DATEANDTIMEDATASETCREATED)
        end
    ) as refresh_date,
    datediff(
        day,
        max(
            case
                when DV_DATEANDTIMEDATASETCREATED <= current_timestamp()
                then to_date(DV_DATEANDTIMEDATASETCREATED)
            end
        ),
        current_date()
    ) as days_since_refresh,

    'MONTHLY' as frequency_type

from DATA_LAKE.SERVICES_DATA."LSPLCM_LocalStandardPatientLevelContractMonitoring"
)

select *
from measurements

{% if is_incremental() %}

where not exists (
    select 1
    from {{ this }} existing

    where existing.measurement_date = measurements.measurement_date
      and existing.table_id = measurements.table_id
)

{% endif %}
