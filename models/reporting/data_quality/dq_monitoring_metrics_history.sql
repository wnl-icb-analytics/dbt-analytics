{{
    config(
        materialized='incremental',
        unique_key=['measurement_date','table_id']
    )
}}

/* Run daily to build data quality metrics history */

with measurements as (

    -- =====================================================
    -- CSDS Referral
    -- Governed CSDS referral fact
    -- =====================================================

    select
        current_date() as measurement_date,
        current_timestamp() as measured_at,
        'CSDS_REFERRAL' as table_id,
        count(*) as row_count,
        max(to_date(source_file_received_at)) as refresh_date,
        datediff(
            day,
            max(to_date(source_file_received_at)),
            current_date()
        ) as days_since_refresh,
        'MONTHLY' as frequency_type

    from {{ ref('fct_csds_referral') }}

    where referral_received_date >= '2021-04-01' --- earlier data is considered unreliable and is not used in reporting


    union all

    -- =====================================================
    -- CSDS Care Contact
    -- Governed one-row-per-contact fact
    -- =====================================================

    select
        current_date() as measurement_date,
        current_timestamp() as measured_at,
        'CSDS_CARE_CONTACT' as table_id,
        count(*) as row_count,
        max(to_date(source_file_received_at)) as refresh_date,
        datediff(
            day,
            max(to_date(source_file_received_at)),
            current_date()
        ) as days_since_refresh,
        'MONTHLY' as frequency_type

    from {{ ref('fct_csds_care_contact') }}


    union all

    -- =====================================================
    -- CSDS Care Activity
    -- Governed one-row-per-activity fact
    -- =====================================================

    select
        current_date() as measurement_date,
        current_timestamp() as measured_at,
        'CSDS_CARE_CONTACT_ACTIVITY' as table_id,
        count(*) as row_count,
        max(to_date(source_file_received_at)) as refresh_date,
        datediff(
            day,
            max(to_date(source_file_received_at)),
            current_date()
        ) as days_since_refresh,
        'MONTHLY' as frequency_type

    from {{ ref('fct_csds_care_activity') }}


    union all

    select
        current_date() as measurement_date,
        current_timestamp() as measured_at,
        'SUS_IP' as table_id,
        count(*) as row_count,
        max(refresh_date) as refresh_date,
        datediff(day,
            max(refresh_date),
            current_date()
        ) as days_since_refresh,
        'DAILY' as frequency_type
    from REPORTING.MAIN_DATA.IP

    union all

    -- =====================================================
    -- SLAM Aggregate Contract Monitoring
    --
    -- Counts rows for the latest submitted
    -- financial period only.
    --
    -- The submission registry determines the valid period,
    -- preventing planned future months from entering the
    -- monthly DQ measurement.
    -- =====================================================

    select
        current_date() as measurement_date,
        current_timestamp() as measured_at,
        'SLAM_LSACM' as table_id,
        count(*) as row_count,

        max(
            case
                when a.dv_dataset_created_at <= current_timestamp()
                then to_date(a.dv_dataset_created_at)
            end
        ) as refresh_date,

        datediff(
            day,
            max(
                case
                    when a.dv_dataset_created_at <= current_timestamp()
                    then to_date(a.dv_dataset_created_at)
                end
            ),
            current_date()
        ) as days_since_refresh,

        'MONTHLY' as frequency_type

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


    union all

    -- =====================================================
    -- SLAM Patient Level Contract Monitoring
    --
    -- Counts rows for the latest submitted
    -- financial period only.
    --
    -- Tiny populations in future months are excluded because
    -- the valid period comes from the submission registry.
    -- =====================================================

    select
        current_date() as measurement_date,
        current_timestamp() as measured_at,
        'SLAM_LSPLCM' as table_id,
        count(*) as row_count,
        max(case
                when p.dv_dataset_created_at <= current_timestamp()
                then to_date(p.dv_dataset_created_at)
            end) as refresh_date,
        datediff(day,
            max(case
                    when p.dv_dataset_created_at <= current_timestamp()
                    then to_date(p.dv_dataset_created_at)
                ),current_date()) as days_since_refresh,
        'MONTHLY' as frequency_type
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
        on try_to_number(left(p.dv_financial_year, 4)) = latest_period.financial_year
       and p.dv_financial_month = latest_period.fy_month_submitted

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
