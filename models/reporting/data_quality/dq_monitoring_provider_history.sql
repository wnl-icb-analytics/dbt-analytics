{{
    config(
        materialized='incremental',
        unique_key=['measurement_date', 'table_id', 'provider_code']
    )
}}

/*
    Daily provider-level monitoring history.

    Captures provider presence and row volume for monitored CSDS tables.

    One row per:
        measurement_date
        table_id
        provider_code
*/

with measurements as (

    -- =====================================================
    -- CSDS Referral
    -- =====================================================

    select
        current_date()                      as measurement_date,
        current_timestamp()                 as measured_at,
        'CSDS_REFERRAL'                     as table_id,
        provider_code,
        provider_name,
        count(*)                            as row_count,
        max(refresh_date)                   as latest_refresh_date

    from REPORTING.MAIN_DATA.CSDS_REFERRAL

    where provider_code is not null

    group by
        provider_code,
        provider_name


    union all


    -- =====================================================
    -- CSDS Care Contact
    -- =====================================================

    select
        current_date()                      as measurement_date,
        current_timestamp()                 as measured_at,
        'CSDS_CARE_CONTACT'                 as table_id,
        provider_code,
        provider_name,
        count(*)                            as row_count,
        max(refresh_date)                   as latest_refresh_date

    from REPORTING.MAIN_DATA.CSDS_CARE_CONTACT

    where provider_code is not null

    group by
        provider_code,
        provider_name


    union all


    -- =====================================================
    -- CSDS Care Contact Activity
    -- =====================================================

    select
        current_date()                      as measurement_date,
        current_timestamp()                 as measured_at,
        'CSDS_CARE_CONTACT_ACTIVITY'        as table_id,
        provider_code,
        provider_name,
        count(*)                            as row_count,
        max(created_date)                   as latest_refresh_date

    from REPORTING.MAIN_DATA.CSDS_CARE_CONTACT_ACTIVITY

    where provider_code is not null

    group by
        provider_code,
        provider_name

    union all

    -- =====================================================
    -- SUS Unified Inpatient
    -- Daily operational feed
    -- =====================================================

    select
        current_date()              as measurement_date,
        current_timestamp()         as measured_at,
        'SUS_IP'                    as table_id,
        provider_code,
        provider_name,
        count(*)                    as row_count,
        max(refresh_date)           as latest_refresh_date

    from REPORTING.MAIN_DATA.IP

    where provider_code is not null

    group by
        provider_code,
        provider_name

    union all

    -- =====================================================
    -- SLAM Local Standard Aggregate Contract Monitoring
    -- Monthly aggregate contract monitoring
    -- =====================================================

    select
        current_date()                          as measurement_date,
        current_timestamp()                     as measured_at,
        'SLAM_LSACM'                            as table_id,
        "Dv_ProviderCode"                       as provider_code,
        "Dv_ProviderCode"                       as provider_name,
        count(*)                                as row_count,

        max(
            case
                when DV_DATEANDTIMEDATASETCREATED <= current_timestamp()
                then to_date(DV_DATEANDTIMEDATASETCREATED)
            end
        )                                       as latest_refresh_date

    from DATA_LAKE.SERVICES_DATA."LSACM_LocalStandardAggregateContractMonitoring"

    where "Dv_ProviderCode" is not null

    group by
        "Dv_ProviderCode"


    union all


    -- =====================================================
    -- SLAM Local Standard Patient Level Contract Monitoring
    -- Monthly patient-level contract monitoring
    --
    -- Invalid future DV_DATEANDTIMEDATASETCREATED values
    -- are excluded from the freshness calculation.
    -- =====================================================

    select
        current_date()                          as measurement_date,
        current_timestamp()                     as measured_at,
        'SLAM_LSPLCM'                           as table_id,
        "Dv_ProviderCode"                       as provider_code,
        "Dv_ProviderCode"                       as provider_name,
        count(*)                                as row_count,

        max(
            case
                when DV_DATEANDTIMEDATASETCREATED <= current_timestamp()
                then to_date(DV_DATEANDTIMEDATASETCREATED)
            end
        )                                       as latest_refresh_date

    from DATA_LAKE.SERVICES_DATA."LSPLCM_LocalStandardPatientLevelContractMonitoring"

    where "Dv_ProviderCode" is not null

    group by
        "Dv_ProviderCode"
)

select *
from measurements

{% if is_incremental() %}

where not exists (

    select 1

    from {{ this }} existing

    where existing.measurement_date = measurements.measurement_date
      and existing.table_id = measurements.table_id
      and existing.provider_code = measurements.provider_code

)

{% endif %}
