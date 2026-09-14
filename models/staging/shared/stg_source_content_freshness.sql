{{ config(materialized='table') }}

/*
One source-derived content-currency signal per profiled DATA_LAKE source.

Dates inside each feed take precedence over Snowflake object-change metadata.
Expected days describe normal delivery cadence. SLA days are contractual and
apply only to OLIDS. Breach days mark a sustained delay that needs attention.
*/

with olids as (
    select
        'DATA_LAKE.OLIDS' as source_schema,
        max(
            case
                when consensus_activity_date::date <= current_date()
                    then consensus_activity_date
            end
        )::date as content_date,
        max(max_lds_transform_datetime)::timestamp_ntz as observed_at,
        'practice_consensus_date' as signal_type,
        'Latest activity date reported by the practice consensus' as signal_detail,
        1 as expected_days,
        5 as sla_days,
        10 as breach_after_days
    from {{ ref('raw_olids_freshness_summary') }}
),

pds_core_dates as (
    select
        max(
            case
                when person_business_effective_from_date::date <= current_date()
                    then person_business_effective_from_date
            end
        ) as content_at
    from {{ ref('raw_pds_pds_person') }}

    union all

    select
        max(
            case
                when usual_address_business_effective_from_date::date <= current_date()
                    then usual_address_business_effective_from_date
            end
        ) as content_at
    from {{ ref('raw_pds_pds_address') }}

    union all

    select
        max(
            case
                when primary_care_provider_business_effective_from_date::date <= current_date()
                    then primary_care_provider_business_effective_from_date
            end
        ) as content_at
    from {{ ref('raw_pds_pds_patient_care_practice') }}
),

pds as (
    select
        'DATA_LAKE.PDS' as source_schema,
        case when count(content_at) = 3 then min(content_at)::date end as content_date,
        max(content_at)::timestamp_ntz as observed_at,
        'core_business_effective_date' as signal_type,
        'Latest business-effective date reached by person, address, and registered-practice data' as signal_detail,
        1 as expected_days,
        null::number as sla_days,
        7 as breach_after_days
    from pds_core_dates
),

deaths_latest_observation as (
    select
        max(dmic_record_valid_date_from)::timestamp_ntz as observed_at,
        max(case when reg_date::date <= current_date() then reg_date end)::date as latest_content_date
    from {{ ref('raw_registries_deaths_deaths') }}
),

deaths_latest_batch as (
    select
        datediff(day, '2000-01-01'::date, reg_date::date) as registration_day
    from {{ ref('raw_registries_deaths_deaths') }}
    cross join deaths_latest_observation
    where dmic_record_valid_date_from = observed_at
        and reg_date::date <= current_date()
),

deaths as (
    select
        'DATA_LAKE.DEATHS' as source_schema,
        dateadd(
            day,
            round(percentile_cont(0.99) within group (order by registration_day))::int,
            '2000-01-01'::date
        )::date as content_date,
        max(observed_at) as observed_at,
        'latest_batch_registration_p99' as signal_type,
        '99th percentile of registration dates in the latest source version' as signal_detail,
        30 as expected_days,
        null::number as sla_days,
        45 as breach_after_days
    from deaths_latest_batch
    cross join deaths_latest_observation
),

sus_receipts as (
    select
        'DATA_LAKE.SUS_UNIFIED_APC' as source_schema,
        coalesce(
            nullif(trim(spell_commissioning_service_agreement_provider_derived), ''),
            nullif(trim(spell_commissioning_service_agreement_provider), '')
        ) as provider,
        system_interchange_received_date::date as receipt_date,
        system_report_query_date::timestamp_ntz as observed_at
    from {{ ref('raw_sus_apc_spell') }}
    where system_interchange_received_date::date <= current_date()

    union all

    select
        'DATA_LAKE.SUS_UNIFIED_OP' as source_schema,
        coalesce(
            nullif(trim(appointment_commissioning_service_agreement_provider_derived), ''),
            nullif(trim(appointment_commissioning_service_agreement_provider), '')
        ) as provider,
        system_interchange_received_date::date as receipt_date,
        system_report_query_date::timestamp_ntz as observed_at
    from {{ ref('raw_sus_op_appointment') }}
    where system_interchange_received_date::date <= current_date()

    union all

    select
        'DATA_LAKE.SUS_UNIFIED_ECDS' as source_schema,
        coalesce(
            nullif(trim(system_record_provider), ''),
            nullif(trim(commissioning_service_agreement_provider), ''),
            nullif(trim(attendance_location_hes_provider_3), '')
        ) as provider,
        system_interchange_received_date::date as receipt_date,
        system_report_query_date::timestamp_ntz as observed_at
    from {{ ref('raw_sus_ecds_emergency_care') }}
    where system_interchange_received_date::date <= current_date()
),

sus_source_latest as (
    select
        source_schema,
        max(receipt_date) as latest_receipt_date,
        max(observed_at) as observed_at
    from sus_receipts
    group by source_schema
),

sus_provider_activity as (
    select
        receipts.source_schema,
        receipts.provider,
        max(receipts.receipt_date) as provider_receipt_date,
        count(*) as recent_record_count
    from sus_receipts as receipts
    inner join sus_source_latest as source_latest
        on receipts.source_schema = source_latest.source_schema
    where receipts.provider is not null
        and receipts.receipt_date >= dateadd(day, -90, source_latest.latest_receipt_date)
    group by receipts.source_schema, receipts.provider
),

sus_provider_coverage as (
    select
        source_schema,
        provider_receipt_date,
        sum(recent_record_count) over (
            partition by source_schema
            order by provider_receipt_date desc
            rows between unbounded preceding and current row
        ) / sum(recent_record_count) over (partition by source_schema) as cumulative_record_share
    from sus_provider_activity
),

sus_source_consensus as (
    select
        source_latest.source_schema,
        coalesce(
            max(case when cumulative_record_share >= 0.90 then provider_receipt_date end),
            max(source_latest.latest_receipt_date)
        )::date as content_date,
        max(source_latest.observed_at) as observed_at
    from sus_source_latest as source_latest
    left join sus_provider_coverage as provider_coverage
        on source_latest.source_schema = provider_coverage.source_schema
    group by source_latest.source_schema
),

sus_apc as (
    select
        source_schema,
        content_date,
        observed_at,
        'provider_volume_consensus_receipt_date' as signal_type,
        'Latest receipt date reached by providers representing 90% of records received in the prior 90 days' as signal_detail,
        7 as expected_days,
        null::number as sla_days,
        14 as breach_after_days
    from sus_source_consensus
    where source_schema = 'DATA_LAKE.SUS_UNIFIED_APC'
),

sus_op as (
    select
        source_schema,
        content_date,
        observed_at,
        'provider_volume_consensus_receipt_date' as signal_type,
        'Latest receipt date reached by providers representing 90% of records received in the prior 90 days' as signal_detail,
        7 as expected_days,
        null::number as sla_days,
        14 as breach_after_days
    from sus_source_consensus
    where source_schema = 'DATA_LAKE.SUS_UNIFIED_OP'
),

sus_ecds as (
    select
        source_schema,
        content_date,
        observed_at,
        'provider_volume_consensus_receipt_date' as signal_type,
        'Latest receipt date reached by providers representing 90% of records received in the prior 90 days' as signal_detail,
        7 as expected_days,
        null::number as sla_days,
        14 as breach_after_days
    from sus_source_consensus
    where source_schema = 'DATA_LAKE.SUS_UNIFIED_ECDS'
),

epd as (
    select
        'DATA_LAKE.EPD_PRIMARY_CARE' as source_schema,
        max(case when rp_end_date::date <= current_date() then rp_end_date end)::date as content_date,
        max(received_date)::timestamp_ntz as observed_at,
        'reporting_period_end' as signal_type,
        'Latest prescribing reporting-period end date in the submission header' as signal_detail,
        30 as expected_days,
        null::number as sla_days,
        45 as breach_after_days
    from {{ ref('raw_epd_pc_medsheader') }}
),

csds as (
    select
        'DATA_LAKE.CSDS' as source_schema,
        max(
            case
                when reporting_period_end_date::date <= current_date()
                    then reporting_period_end_date
            end
        )::date as content_date,
        max(upload_date_time)::timestamp_ntz as observed_at,
        'reporting_period_end' as signal_type,
        'Latest reporting-period end date in the community-services header' as signal_detail,
        30 as expected_days,
        null::number as sla_days,
        45 as breach_after_days
    from {{ ref('raw_csds_cyp000header') }}
),

mhsds as (
    select
        'DATA_LAKE.MHSDS' as source_schema,
        max(
            case
                when reporting_period_end_date::date <= current_date()
                    then reporting_period_end_date
            end
        )::date as content_date,
        max(dmic_date_added)::timestamp_ntz as observed_at,
        'reporting_period_end' as signal_type,
        'Latest reporting-period end date in the mental-health header' as signal_detail,
        30 as expected_days,
        null::number as sla_days,
        45 as breach_after_days
    from {{ ref('raw_mhsds_mhs000header') }}
),

slam_periods as (
    select
        feed,
        last_day(dateadd(
            month,
            dv_financial_month - 1,
            try_to_date(left(dv_financial_year, 4) || '-04-01')
        )) as reporting_period_end,
        submission_loaded_at
    from {{ ref('stg_slam_latest_submission') }}
    where dv_financial_year rlike '^20[0-9]{4}$'
        and dv_financial_month between 1 and 12
),

slam_feed_latest as (
    select
        feed,
        max(
            case
                when reporting_period_end <= current_date()
                    then reporting_period_end
            end
        )::date as content_date,
        max(submission_loaded_at)::timestamp_ntz as observed_at
    from slam_periods
    group by feed
),

slam as (
    select
        'SDL' as source_schema,
        case when count(content_date) = 4 then min(content_date) end as content_date,
        max(observed_at) as observed_at,
        'slam_feed_consensus_month' as signal_type,
        'Latest complete reporting month reached by all four SLAM feeds in SDL' as signal_detail,
        30 as expected_days,
        null::number as sla_days,
        60 as breach_after_days
    from slam_feed_latest
),

waiting_list as (
    select
        'DATA_LAKE.WL' as source_schema,
        max(
            case
                when der_week_ending::date <= current_date()
                    then der_week_ending
            end
        )::date as content_date,
        max(der_submission_date_time_from_dlp)::timestamp_ntz as observed_at,
        'week_ending_date' as signal_type,
        'Latest submitted waiting-list week ending date' as signal_detail,
        7 as expected_days,
        null::number as sla_days,
        14 as breach_after_days
    from {{ ref('raw_wl_wl_submissionlog_data') }}
    where der_is_latest_filetype_provider_weekending = true
),

ers as (
    select
        'DATA_LAKE.ERS' as source_schema,
        max(case when rp_end_date::date <= current_date() then rp_end_date end)::date as content_date,
        max(dmic_date_added)::timestamp_ntz as observed_at,
        'reporting_period_end' as signal_type,
        'Latest e-Referral Service reporting-period end date in the submission header' as signal_detail,
        30 as expected_days,
        null::number as sla_days,
        45 as breach_after_days
    from {{ ref('raw_ers_pc_ebsx00header') }}
),

fact_patient as (
    select
        'DATA_LAKE.FACT_PATIENT' as source_schema,
        max(case when period::date <= current_date() then last_day(period::date) end)::date as content_date,
        null::timestamp_ntz as observed_at,
        'latest_activity_period' as signal_type,
        'Latest monthly activity period; the source table does not expose a load timestamp' as signal_detail,
        30 as expected_days,
        null::number as sla_days,
        60 as breach_after_days
    from {{ ref('raw_fact_patient_factactivity') }}
),

pmct as (
    select
        'DATA_LAKE.PMCT' as source_schema,
        last_day(max(
            case
                when try_to_date(
                    left(split_part(period, '-', 2), 3) || ' ' || split_part(period, '-', 3),
                    'MON YYYY'
                ) <= current_date()
                    then try_to_date(
                        left(split_part(period, '-', 2), 3) || ' ' || split_part(period, '-', 3),
                        'MON YYYY'
                    )
            end
        )) as content_date,
        max(ingested_at)::timestamp_ntz as observed_at,
        'diagnostics_reporting_period' as signal_type,
        'Latest diagnostics reporting month in the current performance presentation feed' as signal_detail,
        30 as expected_days,
        null::number as sla_days,
        60 as breach_after_days
    from {{ ref('raw_performance_diagnosticsmonthlysourceappendreviseprovcomm') }}
),

tat as (
    select
        'DATA_LAKE.TAT' as source_schema,
        last_day(max(
            case
                when data_period::date <= current_date()
                    then data_period
            end
        )) as content_date,
        max(loaded_at)::timestamp_ntz as observed_at,
        'diagnostic_reporting_month' as signal_type,
        'Latest diagnostic turnaround-time reporting month in provider submissions' as signal_detail,
        30 as expected_days,
        null::number as sla_days,
        60 as breach_after_days
    from {{ ref('stg_tat_turnaround_times') }}
),

terminology as (
    select
        'DATA_LAKE.TERMINOLOGY' as source_schema,
        max(case when release_date::date <= current_date() then release_date end)::date as content_date,
        max(run_end)::timestamp_ntz as observed_at,
        'latest_successful_release' as signal_type,
        'Latest terminology release loaded successfully from NHS TRUD' as signal_detail,
        35 as expected_days,
        null::number as sla_days,
        120 as breach_after_days
    from {{ ref('raw_reference_ingest_log') }}
    where lower(status) = 'success'
),

source_signals as (
    select * from olids
    union all
    select * from pds
    union all
    select * from deaths
    union all
    select * from sus_apc
    union all
    select * from sus_op
    union all
    select * from sus_ecds
    union all
    select * from epd
    union all
    select * from csds
    union all
    select * from mhsds
    union all
    select * from slam
    union all
    select * from waiting_list
    union all
    select * from ers
    union all
    select * from fact_patient
    union all
    select * from pmct
    union all
    select * from tat
    union all
    select * from terminology
),

source_latest_dates as (
    select
        'DATA_LAKE.OLIDS' as source_schema,
        max(
            case
                when max_activity_date::date <= current_date()
                    then max_activity_date
            end
        )::date as latest_content_date,
        'latest_activity_date' as latest_signal_type,
        'Latest valid activity date present across OLIDS freshness summary tables' as latest_signal_detail
    from {{ ref('raw_olids_freshness_summary') }}

    union all

    select
        'DATA_LAKE.PDS' as source_schema,
        max(content_at)::date as latest_content_date,
        'latest_core_business_effective_date' as latest_signal_type,
        'Latest valid business-effective date across person, address, or registered-practice data' as latest_signal_detail
    from pds_core_dates

    union all

    select
        'DATA_LAKE.DEATHS' as source_schema,
        max(latest_content_date) as latest_content_date,
        'latest_registration_date' as latest_signal_type,
        'Latest valid registration date present in the source' as latest_signal_detail
    from deaths_latest_observation

    union all

    select
        'SDL' as source_schema,
        max(content_date) as latest_content_date,
        'latest_slam_feed_month' as latest_signal_type,
        'Latest valid reporting month reached by any of the four SLAM feeds in SDL' as latest_signal_detail
    from slam_feed_latest

    union all

    select
        source_schema,
        latest_receipt_date as latest_content_date,
        'latest_record_received_date' as latest_signal_type,
        'Latest valid receipt date present across provider records' as latest_signal_detail
    from sus_source_latest
)

select
    source_signals.source_schema,
    source_signals.content_date,
    coalesce(source_latest_dates.latest_content_date, source_signals.content_date) as latest_content_date,
    source_signals.content_date as consensus_content_date,
    source_signals.observed_at,
    source_signals.signal_type,
    source_signals.signal_detail,
    source_signals.signal_type as consensus_signal_type,
    source_signals.signal_detail as consensus_signal_detail,
    coalesce(source_latest_dates.latest_signal_type, source_signals.signal_type) as latest_signal_type,
    coalesce(source_latest_dates.latest_signal_detail, source_signals.signal_detail) as latest_signal_detail,
    source_signals.expected_days,
    source_signals.sla_days,
    source_signals.breach_after_days
from source_signals
left join source_latest_dates
    on source_signals.source_schema = source_latest_dates.source_schema
