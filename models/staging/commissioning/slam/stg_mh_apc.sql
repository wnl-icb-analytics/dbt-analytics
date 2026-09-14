{{
    config(
        materialized = 'table',
        tags = ['sdl', 'mh_slam', 'mh_apc']
    )
}}

-- Staging model for the MH Local SLAM APC dataset (Appendix 2b: mental health
-- ward stays / occupied bed days). Source: DATA_LAKE.SDL.MH restricted to the
-- '..._MH_<provider>_APC_...' submission files — the MH feed carries both the
-- APC and Non-APC datasets, distinguishable only by file name, so the
-- registry join is the dataset filter (INNER; the regex cannot match the
-- NONAPC token). ~2M rows from RV300 (CNWL) and RKL00 (WLT), Aug 2023
-- onwards. Legacy pre-spec MH files (2015-19) are excluded.
--
-- Cleaned and projected to the 26-field Appendix 2b spec. Grain 1:1 with
-- source: one row per ward stay per activity month (OBDs are the monthly
-- slice of the stay, per the spec's WLT/CNWL counting agreement). Spec fields
-- are near-fully populated (profiled 2026-07): ward_end_date 98% (open
-- stays), gp_practice_code 92%, dv_year_of_birth 70%, service_group 2%
-- (spec-optional).
--
-- Financial period uses the provider-stated DLP value (bare 4-digit start
-- year), falling back to the activity month, then the file name.
--
-- Pseudonymisation (accepted deviation from spec): NHS Number is the
-- pseudonymised key, Date of Birth is year only, Postcode is partial.
--
-- Cumulative Flex/Freeze restatements — summing double-counts; report from
-- stg_mh_apc_latest for the current statement per (provider, FY, month).

with {{ mh_slam_registry('MH', '.*_MH_R[A-Z0-9]+_APC_.*') }},

prep as (
    select
        -- META keys (from SDL pipeline, fully reliable)
        {{ community_pld_meta_columns() }},

        -- DLP standard submission fields (spec 1-2, 5)
        {{ clean_flex_or_freeze('dlp_flexor_freeze') }}
                                                as dlp_flex_or_freeze,
        dlp_commissioner_code                   as dlp_commissioner_code,
        dlp_baseline_financial_month            as dlp_baseline_financial_month,

        -- Provider-stated reporting period (spec 3-4; DLP columns only)
        {{ parse_slam_financial_month('dlp_financial_month') }}
                                                as dv_financial_month_stated,
        {{ fin_year_from_start_year('dlp_financial_year') }}
                                                as dv_financial_year_stated,

        -- 6: Commissioner (also carried as dlp_commissioner_code)
        upper(trim(organisation_identifier_code_of_commissioner))
                                                as commissioner_code,

        -- 7-8: Ward stay dates (end NULL while the stay is open)
        {{ parse_uk_date('ward_start_date') }}  as ward_start_date,
        {{ parse_uk_date('ward_end_date') }}    as ward_end_date,

        -- 9-10: Person (pseudonymised year of birth), ethnicity
        try_to_number(dv_yearof_birth)          as dv_year_of_birth,
        {{ nhs_ethnicity_category_code('ethnic_category') }}
                                                as ethnic_category_code,

        -- 11: Extract date
        {{ parse_uk_date('extract_date') }}     as extract_date,

        -- 12: Gender identity (MH national set 1/2/3/4/X/Z)
        {{ nc_mh_gender_identity('gender_identity_code') }}
                                                as gender_identity_code,

        -- 13: Registered GP practice (validated ODS shape; the spec's
        -- WLT/CNWL agreement splits OBDs by GP practice)
        {{ clean_gp_practice_code('general_medical_practice_code_patient_registration') }}
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

        -- 18: Activity month (first day of the financial month, per spec)
        {{ parse_uk_date('activity_month') }}   as activity_month,

        -- 19: Hospital bed type (MH admitted patient classification, n2
        -- national codes 10-40)
        {{ nc_pad('hospital_bed_type_mental_health_mental_health_admitted_patient_classification', 2) }}
                                                as bed_type_code,

        -- 20-22: Ward and spell identifiers
        nullif(trim(ward_name), '')             as ward_name,
        nullif(trim(ward_code), '')             as ward_code,
        hospital_provider_spell_identifier      as hospital_provider_spell_id,

        -- 23-25: Local service/team detail
        nullif(trim(service_group), '')         as service_group,
        nullif(trim(team_code), '')             as team_code,
        nullif(trim(team_name), '')             as team_name,

        -- 26: Occupied bed days excluding leave (the measure; provider
        -- counting conventions differ per the spec's WLT/CNWL agreement)
        try_to_number(total_occupied_bed_days_excluding_leave)
                                                as occupied_bed_days,

        -- Reporting month parsed from the submission file name (last resort)
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

-- Final projection; period precedence: stated -> activity month -> file name.
select
    -- META keys
    meta_sk_row_id, meta_file_id, meta_row_id, meta_batch_id,
    meta_partition_date, meta_provider_code, meta_recipient_code, meta_version_id,

    -- Reporting period (derived)
    {{ community_pld_financial_period('activity_month', 'activity_month') }},

    -- DLP standard fields
    dlp_flex_or_freeze, dlp_commissioner_code, dlp_baseline_financial_month,

    -- Spec body (fields 6-26, in spec order)
    commissioner_code, ward_start_date, ward_end_date, dv_year_of_birth,
    ethnic_category_code, extract_date, gender_identity_code, gp_practice_code,
    local_patient_id, sk_patient_id, partial_postcode, lsoa, provider_code,
    activity_month, bed_type_code, ward_name, ward_code,
    hospital_provider_spell_id, service_group, team_code, team_name,
    occupied_bed_days,

    -- Raw period (traceability)
    financial_year_raw, financial_month_raw
from prep
