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
    -- SLAM Aggregate Contract Monitoring
    --
    -- Provider row counts for the latest submitted
    -- financial period only.
    -- =====================================================

    select
        current_date()                    as measurement_date,
        current_timestamp()               as measured_at,
        'SLAM_LSACM'                      as table_id,
        a.dv_provider_code                as provider_code,
        a.dv_provider_code                as provider_name,
        count(*)                          as row_count,

        max(
            case
                when a.dv_dataset_created_at <= current_timestamp()
                then to_date(a.dv_dataset_created_at)
            end
        )                                 as latest_refresh_date,

        a.dv_financial_year               as latest_actual_financial_year,
        a.dv_financial_month              as latest_actual_financial_month,

        max(
            to_date(a.submission_loaded_at)
        )                                 as latest_update_date

    from {{ ref('stg_lsacm_latest') }} a

    inner join (

        select
            financial_year,
            fy_month_submitted

        from {{ ref('int_ls_contract_dataset_submissions') }}

        where dataset = 'LSACM'
          and financial_year is not null
          and fy_month_submitted is not null

        qualify row_number() over (
            order by
                financial_year desc,
                fy_month_submitted desc
        ) = 1

    ) latest_period
        on try_to_number(
            left(a.dv_financial_year, 4)
        ) = latest_period.financial_year

       and a.dv_financial_month
            = latest_period.fy_month_submitted

    where a.dv_provider_code is not null

    group by
        a.dv_provider_code,
        a.dv_financial_year,
        a.dv_financial_month


    union all

    -- =====================================================
    -- SLAM Patient Level Contract Monitoring
    --
    -- Provider row counts for the latest submitted
    -- financial period only.
    -- =====================================================

    select
        current_date()                    as measurement_date,
        current_timestamp()               as measured_at,
        'SLAM_LSPLCM'                     as table_id,
        p.dv_provider_code                as provider_code,
        p.dv_provider_code                as provider_name,
        count(*)                          as row_count,

        max(
            case
                when p.dv_dataset_created_at <= current_timestamp()
                then to_date(p.dv_dataset_created_at)
            end
        )                                 as latest_refresh_date,

        p.dv_financial_year               as latest_actual_financial_year,
        p.dv_financial_month              as latest_actual_financial_month,

        max(
            to_date(p.submission_loaded_at)
        )                                 as latest_update_date

    from {{ ref('stg_lsplcm_latest') }} p

    inner join (

        select
            financial_year,
            fy_month_submitted

        from {{ ref('int_ls_contract_dataset_submissions') }}

        where dataset = 'LSPLCM'
          and financial_year is not null
          and fy_month_submitted is not null

        qualify row_number() over (
            order by
                financial_year desc,
                fy_month_submitted desc
        ) = 1

    ) latest_period
        on try_to_number(
            left(p.dv_financial_year, 4)
        ) = latest_period.financial_year

       and p.dv_financial_month
            = latest_period.fy_month_submitted

    where p.dv_provider_code is not null

    group by
        p.dv_provider_code,
        p.dv_financial_year,
        p.dv_financial_month

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
