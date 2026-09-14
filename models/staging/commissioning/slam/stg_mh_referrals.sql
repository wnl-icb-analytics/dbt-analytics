{{
    config(
        materialized = 'table',
        tags = ['sdl', 'mh_slam', 'mh_referrals']
    )
}}

-- Staging model for the MH Local SLAM Referrals dataset (Appendix 2c).
-- Source: DATA_LAKE.SDL.REF restricted to the 'MHRef' submission profile —
-- MH referrals travel in the REF feed (not MH) alongside the community and
-- other referral profiles, so the registry join is the dataset filter
-- (INNER). ~5M rows from RV300 (CNWL) and RKL00 (WLT). The related LPSRef
-- profile (Liaison Psychiatry) is a separate dataset and is not included.
--
-- Cleaned and projected to the 27-field Appendix 2c spec. Grain 1:1 with
-- source: one row per referral per ACTIVE MONTH per submission (a monthly
-- caseload snapshot, ~4 rows per referral even latest-only) — an open
-- referral is restated for every month it is active, with closure fields
-- populating on the final rows. Count referrals via count(distinct
-- referral_id) or the dv_is_current_referral_row flag on
-- stg_mh_referrals_latest, never count(*). Two layout eras (profiled
-- 2026-07): the current spec layout (~3.2M rows, all spec fields incl. CAMHS
-- flag, clinical response priority, closure reason) and older MHRef layouts
-- (~1.8M rows) whose values arrive under sibling aliases — each field
-- coalesces the spec column with its best-populated siblings. Fields the old
-- layouts never carried (ethnic category, GP practice, CAMHS flag, closure
-- reason, extract date) are NULL there.
--
-- Coded fields normalised to the spec's national code sets; note the MH
-- clinical response priority (1 Emergency, 2 Urgent, 3 Routine, 4 Very
-- Urgent) is NOT the community priority set (where Routine = 1). Source of
-- referral is the MH alphanumeric set (A1-Q2) and passes through upper-cased.
--
-- Financial period uses the provider-stated value, falling back to the
-- referral received date, then the file name.
--
-- Pseudonymisation (accepted deviation from spec): NHS Number is the
-- pseudonymised key, Date of Birth is year only, Postcode is partial.
--
-- Cumulative Flex/Freeze restatements — summing double-counts; report from
-- stg_mh_referrals_latest for the current statement per (provider, FY, month).

with {{ mh_slam_registry('REF', '.*MHRef.*') }},

prep as (
    select
        -- META keys (from SDL pipeline, fully reliable)
        {{ community_pld_meta_columns() }},

        -- DLP standard submission fields (spec 1-2, 5)
        {{ clean_flex_or_freeze('dlp_flexor_freeze') }}
                                                as dlp_flex_or_freeze,
        dlp_commissioner_code                   as dlp_commissioner_code,
        dlp_baseline_financial_month            as dlp_baseline_financial_month,

        -- Provider-stated reporting period (spec 3-4; REF carries plain
        -- financial_* siblings alongside the DLP columns)
        {{ community_pld_stated_period() }},

        -- 6: Commissioner (also carried as dlp_commissioner_code)
        upper(trim(organisation_identifier_code_of_commissioner))
                                                as commissioner_code,

        -- 7-8: Person (pseudonymised year of birth), ethnicity (old layouts
        -- never carried it)
        try_to_number(dv_yearof_birth)          as dv_year_of_birth,
        {{ nhs_ethnicity_category_code('ethnic_category') }}
                                                as ethnic_category_code,

        -- 9: Extract date
        {{ parse_uk_date('extract_date') }}     as extract_date,

        -- 10: Gender identity (MH national set; old layouts used sibling
        -- gender columns)
        {{ nc_mh_gender_identity('coalesce(gender_identity_code, person_stated_gender_code, gender)') }}
                                                as gender_identity_code,

        -- 11: Registered GP practice (validated ODS shape)
        {{ clean_gp_practice_code('coalesce(general_medical_practice_code_patient_registration, general_practice_patient_registration, gp_practice_code)') }}
                                                as gp_practice_code,

        -- 12-14: Patient identifiers (pseudonymised)
        coalesce(
            local_patient_identifier,
            local_patient_identifier_extended,
            local_patient_id,
            localpatientid
        )                                       as local_patient_id,
        coalesce(
            {{ consistent_sk_patient_id_format('sk_patient_id_nhs_number') }},
            {{ consistent_sk_patient_id_format('sk_patient_id_nhsnumber') }},
            {{ consistent_sk_patient_id_format('sk_patient_id_nhs_no') }}
        )                                       as sk_patient_id,
        dv_partial_post_code                    as partial_postcode,
        -- LSOA (pipeline-derived geography; not a spec field but high-value)
        nullif(trim(dv_lsoa), '')               as lsoa,

        -- 15: Provider (cleaned ODS code, falling back to the pipeline value)
        {{ clean_organisation_id('upper(trim(coalesce(organisation_identifier_code_of_provider, provider_code, meta_provider_code)))') }}
                                                as provider_code,

        -- 16-18: Local service/team detail (old layouts: descriptions only)
        nullif(trim(service_group), '')         as service_group,
        nullif(trim(team_code), '')             as team_code,
        nullif(trim(coalesce(team_name, team_referred_to_desc, team_referred_to_description, lps_team_name)), '')
                                                as team_name,

        -- 19-20: Referral received and service discharge dates (a populated
        -- discharge date means the referral is closed)
        {{ parse_uk_date('referral_request_received_date') }}
                                                as referral_received_date,
        {{ parse_uk_date('coalesce(service_discharge_date, discharge_date, referral_closure_date, date_discharge)') }}
                                                as service_discharge_date,

        -- 21: Service/team type (an3 national, e.g. A06; local codes pass
        -- through; old layouts used sibling columns)
        upper(nullif(trim(coalesce(service_or_team_type, service_type_requested_code, team_referred_to_code, team_type)), ''))
                                                as team_type_code,

        -- 22: Source of referral (MH alphanumeric national set A1-Q2; local
        -- codes pass through)
        upper(nullif(trim(coalesce(source_of_referral, referral_source_code, source_of_referral_code, referral_source)), ''))
                                                as source_of_referral_code,

        -- 23: Primary reason for referral (n2 national 01-31, zero-padded;
        -- local codes / free text pass through)
        {{ nc_pad('coalesce(primary_reason_for_referral, reason_for_referral_code, referral_reason_code, primary_referral_reason)', 2) }}
                                                as primary_referral_reason_code,

        -- 24: CAMHS flag (1 CAMHS, 0 not; current layout only)
        try_to_number(camhs_flag)               as camhs_flag,

        -- 25: Referral ID
        coalesce(
            referral_id,
            service_request_identifier,
            local_referral_identifier,
            referral_identifier,
            local_referral_id
        )                                       as referral_id,

        -- 26: Clinical response priority (MH national set: 1 Emergency,
        -- 2 Urgent/serious, 3 Routine, 4 Very Urgent)
        {{ nc_mh_priority('coalesce(clinical_response_priority_type, priority_type_code, referral_priority, referralpriority, priority)') }}
                                                as clinical_priority_code,

        -- 27: Referral closure reason (n2 national 01-09, zero-padded)
        {{ nc_pad('coalesce(referral_closure_reason, referral_closure_reason_code)', 2) }}
                                                as referral_closure_reason_code,

        -- Reporting month parsed from the submission file name (last resort)
        {{ period_from_file_name('registry.original_file_name') }}
                                                as file_name_period,

        -- Raw period values retained for traceability
        coalesce(dlp_financial_year, financial_year)    as financial_year_raw,
        coalesce(dlp_financial_month, financial_month)  as financial_month_raw

    from {{ ref('raw_sdl_wnl_ref') }}
    join registry
        on registry.file_id = meta_file_id
       and registry.batch_id = meta_batch_id
)

-- Final projection; period precedence: stated -> referral date -> file name.
select
    -- META keys
    meta_sk_row_id, meta_file_id, meta_row_id, meta_batch_id,
    meta_partition_date, meta_provider_code, meta_recipient_code, meta_version_id,

    -- Reporting period (derived)
    {{ community_pld_financial_period('referral_received_date', 'referral_date') }},

    -- DLP standard fields
    dlp_flex_or_freeze, dlp_commissioner_code, dlp_baseline_financial_month,

    -- Spec body (fields 6-27, in spec order)
    commissioner_code, dv_year_of_birth, ethnic_category_code, extract_date,
    gender_identity_code, gp_practice_code, local_patient_id, sk_patient_id,
    partial_postcode, lsoa, provider_code, service_group, team_code, team_name,
    referral_received_date, service_discharge_date, team_type_code,
    source_of_referral_code, primary_referral_reason_code, camhs_flag,
    referral_id, clinical_priority_code, referral_closure_reason_code,

    -- Raw period (traceability)
    financial_year_raw, financial_month_raw
from prep
