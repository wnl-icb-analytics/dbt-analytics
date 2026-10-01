{{
    config(
        materialized = 'table',
        tags = ['tat']
    )
}}

-- Staged imaging Turnaround Times. Typed + normalised from the all-STRING raw landing
-- (DATA_LAKE.TAT.TURNAROUND_TIMES_RAW). Grain: one row per diagnostic test event,
-- keyed by tat_event_id; exact re-loads of the same file are de-duplicated.
-- Business logic (Flex/Freeze classification, TAT-hour calcs) lives in
-- int_tat_turnaround_times. Analysts should query that model, not this table
-- and not DATA_LAKE__NCL.ANALYST_MANAGED.TURNAROUND_TIMES_RAW.

with raw as (

    select * from {{ ref('raw_tat_turnaround_times_raw') }}

),

normalised as (

    select
        -- provenance / filename (strip stage path + optional .gz)
        regexp_replace(split_part(source_file, '/', -1), '\\.gz$', '')           as file_name,
        source_file,
        loaded_at,

        -- string fields (empty -> null)
        nullif(trim(ethnic_category), '')                                        as ethnic_category,
        nullif(trim(person_gender), '')                                          as person_gender,
        nullif(trim(general_medical_practice), '')                               as general_medical_practice,
        nullif(trim(referrer_code), '')                                          as referrer_code,
        nullif(trim(cancer_pathway_flag), '')                                    as cancer_pathway_flag,

        -- header-spelling variants reconciled
        coalesce(nullif(trim(referring_org), ''), nullif(trim(referring_organisation), ''))   as referring_organisation,
        coalesce(nullif(trim(imaging_code_nicip), ''), nullif(trim(imaging_code_nicip_1), '')) as imaging_code_nicip,
        coalesce(nullif(trim(imaging_code_snomed), ''), nullif(trim(imaging_code_snomed_1), '')) as imaging_code_snomed,
        coalesce(nullif(trim(provider_site_code), ''), nullif(trim(provider_site_code_1), ''))   as provider_site_code,

        -- numerics
        try_to_number(patient_source_type)                                       as patient_source_type,
        try_to_number(priority_type_code)                                        as priority_type_code,

        -- datetimes: providers send UK order 'DD/MM/YYYY HH:MI'; xlsx-converted files arrive ISO.
        {{ tat_parse_ts('diagnostic_test_request_date_time') }}                  as diagnostic_test_request_date_time,
        {{ tat_parse_ts('diagnostic_test_request_received_date_time') }}         as diagnostic_test_request_received_date_time,
        {{ tat_parse_ts('diagnostic_test_date_time') }}                          as diagnostic_test_date_time,
        {{ tat_parse_ts('service_report_issue_date_time') }}                     as service_report_issue_date_time

    from raw

),

derived as (

    select
        *,
        -- identity derived from the filename (YYYYMMDD_DIDNCL_XXX_MonYY[YY])
        try_to_date(left(file_name, 8), 'YYYYMMDD')                              as submission_date,
        substr(file_name, 17, 3)                                                 as trust_code,
        left(initcap(regexp_substr(file_name, '_([A-Za-z]{3,9})(\\d{2,4})', 1, 1, 'e', 1)), 3) as period_month,
        regexp_substr(file_name, '_([A-Za-z]{3,9})(\\d{2,4})', 1, 1, 'e', 2)                    as period_year_raw
    from normalised

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'file_name', 'trust_code',
            'diagnostic_test_request_date_time', 'diagnostic_test_date_time', 'service_report_issue_date_time',
            'imaging_code_nicip', 'imaging_code_snomed', 'provider_site_code',
            'general_medical_practice', 'referrer_code', 'ethnic_category', 'person_gender'
        ]) }}                                                                    as tat_event_id,

        trust_code,
        submission_date,
        period_month                                                            as month,
        iff(len(period_year_raw) = 4, period_year_raw, '20' || period_year_raw) as year,
        try_to_date('01' || period_month || right(iff(len(period_year_raw) = 4, period_year_raw, '20' || period_year_raw), 2), 'DDMONYY') as data_period,

        ethnic_category,
        person_gender,
        general_medical_practice,
        patient_source_type,
        referrer_code,
        referring_organisation,
        diagnostic_test_request_date_time,
        diagnostic_test_request_received_date_time,
        diagnostic_test_date_time,
        service_report_issue_date_time,
        imaging_code_nicip,
        imaging_code_snomed,
        coalesce(imaging_code_nicip, imaging_code_snomed)                        as combined_imaging_code,
        provider_site_code,
        priority_type_code,
        cancer_pathway_flag,

        file_name,
        source_file,
        loaded_at

    from derived

    -- grain guarantee: one row per test event; drop exact re-loads (keep most recent load)
    qualify row_number() over (partition by tat_event_id order by loaded_at desc) = 1

)

select * from final
