{{
    config(
        materialized='incremental',
        unique_key=['measurement_date', 'table_id', 'provider_code'],
        on_schema_change='append_new_columns'
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
        max(refresh_date)                   as latest_refresh_date,

    -- SLAM-specific provider period fields (not used here in CSDS tables)
    null::number                    as latest_actual_financial_year,
    null::number                    as latest_actual_financial_month,
    null::date                      as latest_update_date

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
    current_date()                  as measurement_date,
    current_timestamp()             as measured_at,
    'CSDS_CARE_CONTACT'             as table_id,
    provider_code,
    provider_name,
    count(*)                        as row_count,
    max(refresh_date)               as latest_refresh_date,

    -- SLAM-specific provider period fields (not used here in CSDS tables)
    null::number                    as latest_actual_financial_year,
    null::number                    as latest_actual_financial_month,
    null::date                      as latest_update_date

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
        max(created_date)                   as latest_refresh_date,

   -- SLAM-specific provider period fields (not used here in CSDS tables)
    null::number                    as latest_actual_financial_year,
    null::number                    as latest_actual_financial_month,
    null::date                      as latest_update_date

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
        max(refresh_date)           as latest_refresh_date,

    -- SLAM-specific provider period fields (not used here in SUS Unified tables)
    null::number                    as latest_actual_financial_year,
    null::number                    as latest_actual_financial_month,
    null::date                      as latest_update_date

    from REPORTING.MAIN_DATA.IP

    where provider_code is not null

    group by
        provider_code,
        provider_name

    union all

    -- =====================================================
    -- SLAM Local Standard Aggregate Contract Monitoring
    -- Monthly aggregate contract monitoring
    --
    -- Latest actual financial period is based only on
    -- periods containing > 0 actual activity, preventing
    -- future planned months from being treated as received
    -- actual activity.
    -- =====================================================

    select
        current_date()                       as measurement_date,
        current_timestamp()                  as measured_at,
        'SLAM_LSACM'                         as table_id,
        a."Dv_ProviderCode"                  as provider_code,
        a."Dv_ProviderCode"                  as provider_name,
        count(*)                             as row_count,

        max(
            case
                when a.DV_DATEANDTIMEDATASETCREATED <= current_timestamp()
                then to_date(a.DV_DATEANDTIMEDATASETCREATED)
            end
        )                                    as latest_refresh_date,

        max(
            case
                when coalesce(
                    a."CONTRACT MONITORING ACTUAL ACTIVITY",
                    0
                ) > 0
                then a.DV_FINANCIALYEAR
            end
        )                                    as latest_actual_financial_year,

        max(
            case
                when coalesce(
                    a."CONTRACT MONITORING ACTUAL ACTIVITY",
                    0
                ) > 0

                and a.DV_FINANCIALYEAR = (
                    select max(y.DV_FINANCIALYEAR)
                    from DATA_LAKE.SERVICES_DATA
                        ."LSACM_LocalStandardAggregateContractMonitoring" y

                    where y."Dv_ProviderCode" = a."Dv_ProviderCode"

                      and coalesce(
                          y."CONTRACT MONITORING ACTUAL ACTIVITY",
                          0
                      ) > 0
                )

                then a.DV_FINANCIALMONTH
            end
        )                                    as latest_actual_financial_month,

        max(to_date(a."LastUpdateTime"))     as latest_update_date

    from DATA_LAKE.SERVICES_DATA
        ."LSACM_LocalStandardAggregateContractMonitoring" a

    where a."Dv_ProviderCode" is not null

    group by
        a."Dv_ProviderCode"


        union all

    -- =====================================================
    -- SLAM Local Standard Patient Level Contract Monitoring
    -- Monthly patient-level contract monitoring
    --
    -- Invalid future dataset-created dates are excluded
    -- from the freshness calculation.
    -- =====================================================

    select
        current_date()                       as measurement_date,
        current_timestamp()                  as measured_at,
        'SLAM_LSPLCM'                        as table_id,
        p."Dv_ProviderCode"                  as provider_code,
        p."Dv_ProviderCode"                  as provider_name,
        count(*)                             as row_count,

        max(
            case
                when p.DV_DATEANDTIMEDATASETCREATED <= current_timestamp()
                then to_date(p.DV_DATEANDTIMEDATASETCREATED)
            end
        )                                    as latest_refresh_date,
        max(p.DV_FINANCIALYEAR)              as latest_actual_financial_year,
        max(
            case
                when p.DV_FINANCIALYEAR = (
                    select max(y.DV_FINANCIALYEAR)

                    from DATA_LAKE.SERVICES_DATA
                        ."LSPLCM_LocalStandardPatientLevelContractMonitoring" y

                    where y."Dv_ProviderCode" = p."Dv_ProviderCode"
                )

                then p.DV_FINANCIALMONTH
            end
        )                                    as latest_actual_financial_month,
        max(to_date(p."LastUpdateTime"))     as latest_update_date

    from DATA_LAKE.SERVICES_DATA
        ."LSPLCM_LocalStandardPatientLevelContractMonitoring" p
    where p."Dv_ProviderCode" is not null
    group by
        p."Dv_ProviderCode"
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
