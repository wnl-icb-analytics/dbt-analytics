{{
    config(
        materialized = 'table',
        tags = ['sdl', 'mh_slam', 'mh_nonapc']
    )
}}

-- Staging model for the MH Local SLAM Non-APC dataset (Appendix 2a: mental
-- health care contacts). Source: DATA_LAKE.SDL.MH restricted to the
-- '..._MH_<provider>_NONAPC_...' submission files — the MH feed carries both
-- the Non-APC and APC datasets, distinguishable only by file name, so the
-- registry join is the dataset filter (INNER). ~10M rows from RV300 (CNWL)
-- and RKL00 (WLT), Aug 2023 onwards. Legacy pre-spec MH files (2015-19, no
-- dataset token) are excluded.
--
-- Cleaned and projected to the 28-field Appendix 2a spec. Grain 1:1 with
-- source (one row per care contact). Spec fields are near-fully populated
-- (profiled 2026-07): only service_group (38%, spec-optional) and
-- dv_year_of_birth (88%) fall short.
--
-- Coded fields are normalised to the spec's national code sets: gender
-- identity (MH set incl. 3 Non-binary / Z Not Stated), ethnic category,
-- consultation mechanism and attend/DNA (leading zeros stripped — these sets
-- are not fixed-width), service/team type passed through upper-cased (an3,
-- e.g. A06). Dates parsed across the observed UK/ISO formats.
--
-- Financial period (dv_financial_year/month) uses the provider-stated DLP
-- value (bare 4-digit start year, as the community feeds), falling back to
-- the care contact date, then the file name. dv_financial_period_source
-- records which was used.
--
-- Pseudonymisation (accepted deviation from spec): NHS Number is the
-- pseudonymised key (SK_PATIENT_ID_NHS_NUMBER), Date of Birth is year only,
-- Postcode is partial.
--
-- Cumulative Flex/Freeze restatements — summing double-counts; report from
-- stg_mh_nonapc_latest for the current statement per (provider, FY, month).

with {{ mh_slam_registry('MH', '.*_MH_R[A-Z0-9]+_NONAPC_.*') }},

prep as (
    select
        -- META keys (from SDL pipeline, fully reliable)
        {{ community_pld_meta_columns() }},

        -- DLP standard submission fields (spec 1-2, 5)
        {{ clean_flex_or_freeze('dlp_flexor_freeze') }}
                                                as dlp_flex_or_freeze,
        dlp_commissioner_code                   as dlp_commissioner_code,
        dlp_baseline_financial_month            as dlp_baseline_financial_month,

        -- Provider-stated reporting period (spec 3-4; DLP columns only — the
        -- MH feed has no plain financial_* siblings). FY is a bare 4-digit
        -- start year (e.g. 2025 = FY2025/26).
        {{ parse_slam_financial_month('dlp_financial_month') }}
                                                as dv_financial_month_stated,
        {{ fin_year_from_start_year('dlp_financial_year') }}
                                                as dv_financial_year_stated,

        -- 6: Commissioner (also carried as dlp_commissioner_code)
        upper(trim(organisation_identifier_code_of_commissioner))
                                                as commissioner_code,

        -- 7: Care contact date
        {{ parse_uk_date('care_contact_date') }} as contact_date,

        -- 8-9: Person (pseudonymised year of birth), ethnicity
        try_to_number(dv_yearof_birth)          as dv_year_of_birth,
        {{ nhs_ethnicity_category_code('ethnic_category') }}
                                                as ethnic_category_code,

        -- 10: Extract date
        {{ parse_uk_date('extract_date') }}     as extract_date,

        -- 11: Consultation mechanism (MH national set 1-13, 98 — not fixed
        -- width, so leading zeros are stripped: '01' and '1' both arrive)
        {{ nc_strip_zeros('consultation_mechanism_mental_health_code') }}
                                                as consultation_mechanism_code,

        -- 12: Gender identity (MH national set 1/2/3/4/X/Z)
        {{ nc_mh_gender_identity('gender_identity_code') }}
                                                as gender_identity_code,

        -- 13: Registered GP practice at point of contact (validated ODS shape)
        {{ clean_gp_practice_code('general_medical_practice_code_patient_registration_at_point_of_contact') }}
                                                as gp_practice_code,

        -- 14-16: Patient identifiers (pseudonymised)
        local_patient_identifier                as local_patient_id,
        {{ consistent_sk_patient_id_format('sk_patient_id_nhs_number')}} as sk_patient_id,
        dv_partial_post_code                    as partial_postcode,
        -- LSOA (pipeline-derived geography; not a spec field but high-value)
        nullif(trim(dv_lsoa), '')               as lsoa,

        -- 17: Provider (cleaned ODS code, falling back to the pipeline value)
        {{ clean_organisation_id('upper(trim(coalesce(organisation_identifier_code_of_provider, meta_provider_code)))') }}
                                                as provider_code,

        -- 18: Attended or DNA (national single digit 2-7; zeros stripped)
        {{ nc_strip_zeros('attended_or_did_not_attend_code') }}
                                                as attended_or_dna_code,

        -- 19: Activity month (first day of the financial month, per spec)
        {{ parse_uk_date('activity_month') }}   as activity_month,

        -- 20-21: Referral context of the contact
        {{ parse_uk_date('referral_request_received_date') }}
                                                as referral_received_date,
        service_request_identifer_referral_id   as service_request_id,

        -- 22-23: Service/team type (an3 national, e.g. A06; local codes pass
        -- through) and CAMHS flag (1 CAMHS, 0 not)
        upper(nullif(trim(service_or_team_type), ''))
                                                as team_type_code,
        try_to_number(camhs_flag)               as camhs_flag,

        -- 24-26: Local service/team detail
        nullif(trim(service_group), '')         as service_group,
        nullif(trim(team_code), '')             as team_code,
        nullif(trim(team_name), '')             as team_name,

        -- 27-28: Care contact identifiers
        care_contact_identifier                 as care_contact_id,
        care_contact_referral_id                as care_contact_referral_id,

        -- Reporting month parsed from the submission file name (last-resort
        -- period source)
        {{ period_from_file_name('registry.original_file_name') }}
                                                as file_name_period,

        -- Raw period values retained for traceability
        dlp_financial_year                      as financial_year_raw,
        dlp_financial_month                     as financial_month_raw

    from {{ ref('raw_sdl_wnl_mh') }}
    join registry
        on registry.file_id = meta_file_id
       and registry.batch_id = meta_batch_id
)

-- Final projection; period precedence: stated -> contact date -> file name.
select
    -- META keys
    meta_sk_row_id, meta_file_id, meta_row_id, meta_batch_id,
    meta_partition_date, meta_provider_code, meta_recipient_code, meta_version_id,

    -- Reporting period (derived)
    {{ community_pld_financial_period('contact_date', 'contact_date') }},

    -- DLP standard fields
    dlp_flex_or_freeze, dlp_commissioner_code, dlp_baseline_financial_month,

    -- Spec body (fields 6-28, in spec order)
    commissioner_code, contact_date, dv_year_of_birth, ethnic_category_code,
    extract_date, consultation_mechanism_code, gender_identity_code,
    gp_practice_code, local_patient_id, sk_patient_id, partial_postcode, lsoa,
    provider_code, attended_or_dna_code, activity_month,
    referral_received_date, service_request_id, team_type_code, camhs_flag,
    service_group, team_code, team_name, care_contact_id,
    care_contact_referral_id,

    -- Raw period (traceability)
    financial_year_raw, financial_month_raw
from prep
