{{
    config(
        materialized = 'incremental',
        incremental_strategy = 'append',
        on_schema_change = 'append_new_columns',
        tags = ['mhcorl', 'sdl', 'mh']
    )
}}

-- Staging model for the SDL MHCORL feed: provider monthly mental-health
-- activity submissions (spreadsheet uploads remapped by the SDL pipeline).
-- Source: DATA_LAKE.SDL.MHCORL, 228 TEXT columns that are the superset of
-- every provider layout ever submitted. Grain 1:1 with source.
--
-- MHCORL is not one dataset. Each provider sends its own layout, and several
-- send more than one dataset through the same feed (profiled 2026-09,
-- FY2022/23 onwards):
--   * RRP00 (BEH / NLFT): community contacts (NONIP files) and inpatient
--     ward stays (IP files). New DLP-header layout from January 2024.
--   * RNK00 (Tavistock & Portman): appointment-level 'MHCO' dataset. New
--     DLP-header layout from January 2024.
--   * RAT00 (NELFT): spell-level currency rows (treatment / assessment /
--     contact / waiting / OBD).
--   * RKL00 (West London): APPOINTMENTS, INPATIENTS and liaison-psychiatry
--     (LPS, year-to-date) files, to November 2023.
--   * RV300 (CNWL): outpatient / community contacts, APC episodes and LPS
--     files, to September 2023.
-- dv_dataset labels each row inpatient / liaison / community from the
-- submission file name and the provider's own dataset column, so consumers
-- can pick the grain they need. Dataset-specific detail columns that only one
-- layout carries (LPS follow-up contact counts, RNK00 pricing, etc.) stay in
-- the raw model.
--
-- Cleaning follows the other SDL staging models (stg_comopl, stg_mh_apc):
-- best-populated sibling column per field, dates parsed across the provider
-- formats (ISO, UK, DD-Mon-YY, SQL Server AM/PM, Excel serials), coded
-- fields mapped through the shared macros, financial period from the stated
-- DLP / reporting value, else the activity month or contact date, else the
-- file name.
--
-- Cumulative Flex/Freeze restatement feed (RAT00 resubmissions, RKL00 LPS
-- year-to-date files, per-commissioner RNK00 / RV300 files): summing across
-- files double-counts. Report from stg_mhcorl_latest, which keeps the winning
-- file per (dataset, provider, commissioner, FY, month) slice via
-- stg_mhcorl_latest_submission.
--
-- Incremental, pure append: new (file, batch) pairs only, mirroring the
-- upstream SDL loader's NOT EXISTS mechanics (meta_file_id is not monotonic
-- with load time, so a high-water mark would miss back-dated files). Nothing
-- here is mutable; latest-submission resolution is rebuilt fully each run in
-- stg_mhcorl_latest_submission. Full-refresh after an upstream SDL rebuild.
--
-- Scope: FY2022/23 onwards. Earlier submissions have major DQ gaps (2017 has
-- no contact dates, 2020-21 is COVID-degraded, 2021 has no RRP00) and every
-- key field is consistently populated from here on. meta_partition_date is
-- not a calendar month: the SDL pipeline sets it to <FY start year>-<financial
-- month>-01 (checked against DLP_FINANCIAL_YEAR / _MONTH and contact dates,
-- 2026-09), so FY2022/23 month 1 is partition 2022-01-01. The previous
-- cutoff of 2022-04-01 silently dropped April-June 2022.

with {{ community_pld_registry('MHCORL') }},

{% if is_incremental() %}
-- (file, batch) pairs in the raw feed but not yet in this table
new_files as (
    select meta_file_id, meta_batch_id
    from {{ ref('raw_sdl_wnl_mhcorl') }}
    group by all
    minus
    select distinct meta_file_id, meta_batch_id
    from {{ this }}
),
{% endif %}

-- Sibling columns coalesced once so the parsers below are applied to a single
-- expression per field.
src as (
    select
        *,
        coalesce(referral_date, date_of_referral, dateofreferral,
                 lps_referral_date_time, lpsreferraldatetime)
                                                as referral_date_any,
        coalesce(contact_date, contactdate,
                 col_1_stf2_fcontactdatetime, col_1_st_f2_fcontact_date_time)
                                                as contact_date_any,
        coalesce(episode_start_date, ward_start_date, admission_date)
                                                as episode_start_any,
        coalesce(episode_end_date, ward_end_date)
                                                as episode_end_any,
        coalesce(discharge_date, discharge_date_time)
                                                as discharge_date_any,
        coalesce(gp_practice_code, general_practice_code, registered_gp)
                                                as gp_practice_code_any,
        coalesce(ethnicity_code, ethnic_code)   as ethnicity_code_any
    from {{ ref('raw_sdl_wnl_mhcorl') }} as r
    where meta_partition_date >= '2022-01-01'
      -- a spreadsheet header row re-ingested as data
      and coalesce(upper(trim(financial_year)), '') <> 'FINANCIAL YEAR'
{% if is_incremental() %}
      and exists (
          select 1
          from new_files as nf
          where nf.meta_file_id = r.meta_file_id
            and nf.meta_batch_id = r.meta_batch_id
      )
{% endif %}
),

prep as (
    select
        -- META keys (from SDL pipeline, fully reliable)
        {{ community_pld_meta_columns() }},
        dv_recipient_code                       as dv_recipient_code,

        -- Which dataset the row belongs to, from the submission file name
        -- (RRP00 IP / NONIP, RKL00 INPATIENTS / APPOINTMENTS / LPS, RV300 IP /
        -- NON_IP / LIAISON PSYCHIATRY), then the provider's own dataset column
        -- (RKL00 INPATIENT, RV300 APC) and RAT00's OBD currency rows. The
        -- non-inpatient tokens are tested before the inpatient ones because
        -- 'NONINPATIENT' and '_NON_IP' contain them. Everything else is
        -- contact-level.
        case
            when upper(registry.original_file_name) rlike '.*(LPS|LIAISON).*'
                then 'liaison'
            when upper(registry.original_file_name) rlike '.*(NONIP|NON_IP|NON-IP|NONINPATIENT|NON INPATIENT).*'
                then 'community'
            when upper(registry.original_file_name) rlike '.*(_IP_|_IP[.]CSV|_IP - |INPATIENT).*'
              or upper(trim(coalesce(data_set, dataset))) in ('INPATIENT', 'APC')
              or upper(trim(currency)) = 'OBD'
                then 'inpatient'
            else 'community'
        end                                     as dv_dataset,

        -- DLP standard submission fields. The Flex/Freeze flag also appears in
        -- the pre-DLP RRP00 layout (FLEX_OR_FREEZE) and as RAT00's derived
        -- DV_IS_FREEZE boolean.
        coalesce(
            {{ clean_flex_or_freeze('dlp_flexor_freeze') }},
            {{ clean_flex_or_freeze('flex_or_freeze') }},
            case trim(dv_is_freeze) when '1' then 'Freeze' when '0' then 'Flex' end
        )                                       as dlp_flex_or_freeze,
        dlp_commissioner_code                   as dlp_commissioner_code,
        dlp_baseline_financial_month            as dlp_baseline_financial_month,

        -- Provider-stated reporting period. REPORTING_MONTH is the financial
        -- month (it equals DLP_FINANCIAL_MONTH wherever both are present).
        -- Each candidate is validated before coalescing so junk cannot mask a
        -- valid sibling. Financial year formats seen: '2023', '2023/2024',
        -- '2023-2024', '2023_24' (bare year = FY start year, as in the DLP
        -- spec).
        coalesce(
            {{ parse_slam_financial_month('dlp_financial_month') }},
            {{ parse_slam_financial_month('reporting_month') }}
        )                                       as dv_financial_month_stated,
        coalesce(
            {{ parse_slam_financial_year('dlp_financial_year', allow_bare_year=true) }},
            {{ parse_slam_financial_year('financial_year', allow_bare_year=true) }},
            {{ parse_slam_financial_year('fin_year', allow_bare_year=true) }}
        )                                       as dv_financial_year_stated,

        -- Activity month as stated by RKL00 / RV300 (a date, or 'YYYYMM' in
        -- the LPS layouts); the period fallback for rows with no DLP header.
        coalesce(
            {{ parse_uk_date('activity_month') }},
            case
                when coalesce(activitymonth, acivity_month) rlike '^20[0-9]{4}$'
                    then try_to_date(coalesce(activitymonth, acivity_month) || '01', 'YYYYMMDD')
            end
        )                                       as activity_month,

        -- Organisation. The pipeline-assigned META_PROVIDER_CODE is the
        -- provider: the supplied PROVIDER_CODE is empty in two RV300 layouts
        -- and holds a non-trust code in the current RRP00 layout, and equals
        -- the pipeline value everywhere else. Cleaned to the ODS trust code as
        -- in the other SDL staging models. COMMISSIONER_CODE is empty in the
        -- LPS layouts, which carry the commissioner as CCG.
        {{ clean_organisation_id('upper(trim(meta_provider_code))') }}
                                                as provider_code,
        upper(trim(coalesce(commissioner_code, ccg)))
                                                as commissioner_code,
        nullif(trim(commissioner_name), '')     as commissioner_name,
        nullif(trim(site_code), '')             as site_code,

        -- Patient identifiers (pseudonymised). SK_PATIENT_ID_NHS_NUMBER is the
        -- cross-dataset key; the bare SK_PATIENT_ID is a locally derived hash
        -- with no overlap, kept only for within-provider linking.
        coalesce(hospital_number, patient_key)  as local_patient_id,
        {{ consistent_sk_patient_id_format('sk_patient_id_nhs_number') }}
                                                as sk_patient_id,
        sk_patient_id                           as sk_patient_id_local_hash,
        try_to_number(dv_yearof_birth)          as dv_year_of_birth,
        dv_partial_post_code                    as partial_postcode,
        nullif(trim(dv_lsoa), '')               as lsoa,

        -- Demographics. Gender keeps the label set the model has always
        -- exposed; RV300 LPS supplies it as SEX.
        case
            when upper(trim(coalesce(gender, sex))) in ('1', 'M', 'MALE')      then 'Male'
            when upper(trim(coalesce(gender, sex))) in ('2', 'F', 'FEMALE')    then 'Female'
            when upper(trim(coalesce(gender, sex))) in ('9', 'NOT SPECIFIED', 'NOT STATED')
                                                                                then 'Not specified'
            when upper(trim(coalesce(gender, sex))) in ('0', 'U', 'X', 'NOT KNOWN', 'UNKNOWN')
              or upper(trim(coalesce(gender, sex))) like 'NOT KNOWN (%'
                                                                                then 'Not known'
            when upper(trim(coalesce(gender, sex))) like 'INDETERMINATE%'      then 'Indeterminate'
            when upper(trim(coalesce(gender, sex))) in ('NON-BINARY', 'OTHER') then 'Other'
            when nullif(trim(coalesce(gender, sex)), '') is null                then null
            when upper(trim(coalesce(gender, sex))) in ('REMOVEDA', 'GENDER')  then null
            else 'Not known'
        end                                     as gender,
        -- Each ethnicity candidate is mapped before coalescing so a junk code
        -- column cannot mask a mappable free-text sibling.
        coalesce(
            {{ nhs_ethnicity_category_code('ethnicity_code_any') }},
            {{ nhs_ethnicity_category_code('ethnicity') }}
        )                                       as ethnic_category_code,
        {{ clean_gp_practice_code('upper(trim(gp_practice_code_any))') }}
                                                as gp_practice_code,
        nullif(trim(gp_code), '')               as gp_code,

        -- Service / team / contract categorisation (as supplied, siblings
        -- coalesced; local vocabularies differ per provider)
        nullif(trim(coalesce(team_code, lps_team_code)), '')
                                                as team_code,
        nullif(trim(coalesce(team_name, lps_team_name, lpsteamname, team)), '')
                                                as team_name,
        nullif(trim(service_group), '')         as service_group,
        nullif(trim(coalesce(service_line, service_description)), '')
                                                as service_line,
        nullif(trim(pod_code), '')              as pod_code,
        nullif(trim(coalesce(pod_description, pod_name)), '')
                                                as pod_description,
        nullif(trim(contract_type), '')         as contract_type,
        nullif(trim(finance_category), '')      as finance_category,
        nullif(trim(costing_code_description), '')
                                                as costing_code_description,
        nullif(trim(currency), '')              as currency,

        -- Referral
        coalesce(referral_unique_id, referral_ssid, referral_reference)
                                                as referral_id,
        coalesce(
            {{ parse_uk_date('referral_date_any') }},
            {{ parse_slam_timestamp('referral_date_any') }}::date
        )                                       as referral_date,
        nullif(trim(coalesce(source_of_referral, referral_source, referralsource)), '')
                                                as source_of_referral,
        nullif(trim(coalesce(referral_priority, referralpriority)), '')
                                                as referral_priority,

        -- Contact / appointment. RRP00 exports contact dates in four forms
        -- (UK, ISO timestamp, Excel serial, Excel time artefacts); the SLAM
        -- timestamp parser recovers the serials, the artefacts yield NULL.
        appointment_id                          as appointment_id,
        coalesce(
            {{ parse_uk_date('contact_date_any') }},
            {{ parse_slam_timestamp('contact_date_any') }}::date
        )                                       as contact_date,
        nullif(trim(coalesce(consultation_type, contact_type, first_follow_up)), '')
                                                as contact_type,
        nullif(trim(consultation_medium), '')   as consultation_medium,
        nullif(trim(coalesce(contactsetting, col_1_st_f2_fcontact_setting, col_1_stf2_fcontactsetting)), '')
                                                as contact_setting,
        nullif(trim(appointment_type), '')      as appointment_type,
        nullif(trim(appointment_outcome), '')   as appointment_outcome,
        nullif(trim(coalesce(patient_seen, patientseen)), '')
                                                as patient_seen,
        try_to_number(duration_of_contact_minutes)
                                                as duration_of_contact_minutes,
        appointment_sequence_id                 as appointment_sequence_id,
        -- Provider-reported activity counts: RNK00's in-month actual (can be
        -- fractional), RKL00 / RV300's total activity. Different provider
        -- definitions, so kept as separate columns.
        try_to_double(in_month_activity_actual) as in_month_activity_actual,
        try_to_number(total_activity)           as total_activity,

        -- Inpatient / spell block (RRP00 IP, RKL00 INPATIENTS, RV300 APC,
        -- RAT00 spells)
        coalesce(spell_id, care_spell_id)       as spell_id,
        nullif(trim(ward_code), '')             as ward_code,
        nullif(trim(coalesce(ward_name, ward)), '')
                                                as ward_name,
        coalesce(
            {{ parse_uk_date('episode_start_any') }},
            {{ parse_slam_timestamp('episode_start_any') }}::date
        )                                       as episode_start_date,
        coalesce(
            {{ parse_uk_date('episode_end_any') }},
            {{ parse_slam_timestamp('episode_end_any') }}::date
        )                                       as episode_end_date,
        try_to_number(coalesce(occupied_bed_days, obd))
                                                as occupied_bed_days,
        try_to_number(leave_days)               as leave_days,
        coalesce(
            {{ parse_uk_date('discharge_date_any') }},
            {{ parse_slam_timestamp('discharge_date_any') }}::date
        )                                       as discharge_date,
        nullif(trim(discharge_reason), '')      as discharge_reason,

        -- Reporting month parsed from the submission file name (last-resort
        -- period source; few MHCORL file names carry one)
        {{ period_from_file_name('registry.original_file_name') }}
                                                as file_name_period,

        -- Raw period values retained for traceability
        coalesce(dlp_financial_year, financial_year, fin_year)
                                                as financial_year_raw,
        coalesce(dlp_financial_month, reporting_month)
                                                as financial_month_raw

    from src
    left join registry
        on registry.file_id = meta_file_id
       and registry.batch_id = meta_batch_id
),

-- Period fallback date: the provider-stated activity month where the layout
-- has one, else the contact date.
with_period_date as (
    select
        *,
        coalesce(activity_month, contact_date)  as period_activity_date
    from prep
)

select
    -- META keys
    meta_sk_row_id, meta_file_id, meta_row_id, meta_batch_id,
    meta_partition_date, meta_provider_code, meta_recipient_code, meta_version_id,
    dv_recipient_code,

    -- Reporting period (derived: stated -> activity month / contact date -> file name)
    {{ community_pld_financial_period('period_activity_date', 'activity_date') }},

    -- DLP standard fields
    dlp_flex_or_freeze, dlp_commissioner_code, dlp_baseline_financial_month,

    -- Dataset and organisation
    dv_dataset, provider_code, commissioner_code, commissioner_name, site_code,

    -- Patient
    local_patient_id, sk_patient_id, sk_patient_id_local_hash, dv_year_of_birth,
    partial_postcode, lsoa, gender, ethnic_category_code,
    {{ nhs_ethnicity_17_label('ethnic_category_code') }}
                                                as ethnicity_17,
    case
        when ethnic_category_code in ('A', 'B', 'C')       then 'White'
        when ethnic_category_code in ('D', 'E', 'F', 'G')  then 'Mixed'
        when ethnic_category_code in ('H', 'J', 'K', 'L')  then 'Asian or Asian British'
        when ethnic_category_code in ('M', 'N', 'P')       then 'Black or Black British'
        when ethnic_category_code in ('R', 'S')            then 'Other Ethnic Groups'
        when ethnic_category_code in ('Z', '99', '0')      then 'Unknown'
    end                                         as ethnicity_6,
    gp_practice_code, gp_code,

    -- Service / team / contract
    team_code, team_name, service_group, service_line, pod_code, pod_description,
    contract_type, finance_category, costing_code_description, currency,

    -- Referral
    referral_id, referral_date, source_of_referral, referral_priority,

    -- Contact / appointment
    appointment_id, contact_date, contact_type, consultation_medium, contact_setting,
    appointment_type, appointment_outcome, patient_seen, duration_of_contact_minutes,
    appointment_sequence_id, in_month_activity_actual, total_activity,

    -- Inpatient / spell
    spell_id, ward_code, ward_name, episode_start_date, episode_end_date,
    occupied_bed_days, leave_days, discharge_date, discharge_reason,
    activity_month,

    -- Raw period (traceability)
    financial_year_raw, financial_month_raw
from with_period_date
