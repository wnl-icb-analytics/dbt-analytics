{{
    config(
        materialized = 'table',
        tags = ['sdl', 'community_pld', 'comopl']
    )
}}

-- Staging model for the SDL COMOPL feed (Community Outpatient / contact-level
-- activity, the source for the Community SLAM PLD - Appendix 5a). Source:
-- DATA_LAKE.SDL.COMOPL — ~94M rows, 620 cols (superset of every historic
-- provider layout), all TEXT.
--
-- Cleaned and projected to the 28-field Appendix 5a spec. Grain 1:1 with
-- source. Output columns follow the spec order; a downstream view can rename
-- to the exact spec field names and restrict as needed.
--
-- Coded fields are normalised to their national code set where recognisable
-- (gender/priority word maps, zero-padding of leading-zero-stripped numerics,
-- common consultation/cancellation phrasings, free-text ethnicity via the
-- shared nhs_ethnicity_category_code macro proven on stg_mhcorl); genuine
-- provider-local codes pass through unchanged. Validated 2026-06 against the
-- Appendix 5a NationalCodes_* sets: gender/ethnic 100%, priority/team/
-- consultation ~98%, attendance 99%. Source-of-referral (~67%) and cancellation
-- reason (~58%) retain local-code / free-text residue not nationally mappable.
--
-- Financial period (dv_financial_year/month) uses the provider-stated value,
-- falling back to the contact date when absent (the SLAM #806 lesson): lifts
-- coverage ~56% -> ~93%. dv_financial_period_source records which was used.
-- GP practice is cleaned to a valid 6-char ODS code (1 letter + 5 digits).
--
-- Column choice is fill-driven: the spec-named columns are frequently sparse
-- in this superset, so each field coalesces the spec column with the
-- best-populated provider sibling (fill rates profiled 2026-06). Notable cases:
--   * gender: GENDER (56%) carries the data, not PERSON_STATED_GENDER_CODE (0%)
--   * local patient id: LOCAL_PATIENT_ID / LOCAL_PATIENT_IDENTIFIER (~42%),
--     not the spec's LOCAL_PATIENT_IDENTIFIER_EXTENDED (0%)
--   * provider: PROVIDER_CODE (9%) / META_PROVIDER_CODE (100%), not
--     ORGANISATION_IDENTIFIER_CODE_OF_PROVIDER (6%)
--   * contact date / discharge date: coalesced across siblings
--
-- Pseudonymisation (accepted deviation from spec): NHS Number is the
-- pseudonymised key (SK_PATIENT_ID_NHS_NUMBER), Date of Birth is year only
-- (dv_year_of_birth), Postcode is partial (partial_postcode).
--
-- Financial year here is a bare 4-digit year (e.g. 2019 = FY2019/20), unlike
-- the SLAM feeds' YYYYYY — so dv_financial_year is the validated 4-digit year.

with {{ community_pld_registry('COMOPL') }},
{{ community_pld_provider_codes('raw_sdl_wnl_comopl') }},

prep as (
    select
        -- META keys (from SDL pipeline, fully reliable)
        {{ community_pld_meta_columns() }},

        -- DLP standard submission fields (spec 1, 2, 5; only ~56% of rows carry
        -- them). The DLP financial month/year (spec 3-4) are surfaced cleaned as
        -- dv_financial_* and as financial_*_raw, so not repeated here.
        {{ clean_flex_or_freeze('dlp_flexor_freeze') }}
                                                as dlp_flex_or_freeze,
        dlp_commissioner_code                   as dlp_commissioner_code,
        dlp_baseline_financial_month            as dlp_baseline_financial_month,

        -- Provider-stated reporting period; activity-date / file-name fallback
        -- applied in the final select via community_pld_financial_period.
        {{ community_pld_stated_period() }},

        -- 6: Local patient identifier (spec col is empty; siblings carry it)
        coalesce(
            local_patient_identifier_extended,
            local_patient_id,
            local_patient_identifier
        )                                       as local_patient_id,

        -- 7-9: Patient identifiers (pseudonymised)
        sk_patient_id_nhs_number                as sk_patient_id,
        try_to_number(dv_yearof_birth)          as dv_year_of_birth,
        dv_partial_post_code                    as partial_postcode,

        -- 10: Gender -> national code (coalesce provider-variant siblings)
        {{ nc_gender('coalesce(person_stated_gender_code, gender, person_gender_code_current)') }}
                                                as gender_code,
        -- 11: Ethnic category -> NHS national code. Each candidate is mapped
        -- before coalescing so a junk first value can't mask a mappable sibling.
        coalesce(
            {{ nhs_ethnicity_category_code('ethnic_category') }},
            {{ nhs_ethnicity_category_code('patient_ethnicity_code') }},
            {{ nhs_ethnicity_category_code('ethnicity') }}
        )                                       as ethnic_category_code,
        -- 12: Registered GP practice. Accept only a 6-char ODS code (1 letter +
        -- 5 digits) or that plus a 3-digit branch suffix, then take the 6-char
        -- code; reject other lengths / '999' rather than truncating garbage.
        case
            when coalesce(general_medical_practice_code_patient_registration, gp_practice_code, org_id_gp)
                rlike '^[A-Za-z][0-9]{5}([0-9]{3})?$'
            then upper(left(coalesce(general_medical_practice_code_patient_registration, gp_practice_code, org_id_gp), 6))
        end                                     as gp_practice_code,
        -- LSOA (derived geography; not a PLD spec field but high-value, ~89% filled)
        nullif(trim(dv_lsoa), '')               as lsoa,

        -- 13: Source of referral (coalesce siblings; zero-pad national, local pass through)
        {{ nc_pad('coalesce(source_of_referral_for_community, sourceof_referral, referral_source)', 2) }}
                                                as source_of_referral_code,
        -- 14: Referral request received date. The time column is parsed last so
        -- a date misfiled there (e.g. '09/11/2017') is recovered; real times
        -- yield no date and are ignored.
        {{ parse_uk_date('coalesce(referral_date, dateof_referral, referral_received_date, referral_request_received_time)') }}
                                                as referral_received_date,
        -- 15: Referral request received time (clock times only; misfiled dates
        -- excluded here and recovered into the date above)
        {{ clean_time('referral_request_received_time') }}
                                                as referral_received_time,
        -- 16: Service or team type referred to (zero-pad national 01-56)
        {{ nc_pad('coalesce(service_or_team_type_referred_to_community_care, team_type, service_type_referred_to)', 2) }}
                                                as team_type_code,
        -- 17: Priority type -> national code (1 Routine, 2 Urgent, 3 TWW)
        {{ nc_priority('coalesce(priority_type, priority_code)') }}
                                                as priority_type_code,
        -- 18: Care contact date (time column parsed last to recover a misfiled date)
        {{ parse_uk_date('coalesce(care_contact_date, contact_date, care_contact_time)') }}
                                                as contact_date,
        -- 19: Care contact time (clock times only; coalesce siblings)
        {{ clean_time('coalesce(care_contact_time, contact_time)') }}
                                                as contact_time,
        -- 20: Care contact cancellation reason (national: 01 clinical, 02
        -- non-clinical). Map only defensible signals: explicit (non-)clinical
        -- text (separators collapsed so 'non-clinical' / 'non clinical' both
        -- match, checked before plain 'clinical'), and patient-related reasons
        -- (non-clinical). Other free text is left to pass through, not guessed.
        case
            when replace(replace(upper(trim(care_contact_cancellation_reason)), '-', ''), ' ', '') like '%NONCLINICAL%' then '02'
            when replace(replace(upper(trim(care_contact_cancellation_reason)), '-', ''), ' ', '') like '%CLINICAL%'    then '01'
            when upper(trim(care_contact_cancellation_reason)) like '%PATIENT%'                                          then '02'
            else {{ nc_pad('care_contact_cancellation_reason', 2) }}
        end                                     as contact_cancellation_reason,
        -- 21: Consultation type (01 first, 02 follow-up; map words, zero-pad)
        case
            when upper(trim(consultation_type)) like 'FIRST%'                    then '01'
            when upper(trim(consultation_type)) like 'FOLLOW%'                   then '02'
            else {{ nc_pad('consultation_type', 2) }}
        end                                     as consultation_type_code,
        -- 22: Consultation mechanism (coalesce siblings; zero-pad national, map words)
        case
            when upper(trim(coalesce(consultation_medium_used, cons_mechanism))) like 'FACE TO FACE%' then '01'
            when upper(trim(coalesce(consultation_medium_used, cons_mechanism))) like 'TELEPHONE%'    then '02'
            else {{ nc_pad('coalesce(consultation_medium_used, cons_mechanism)', 2) }}
        end                                     as consultation_mechanism_code,
        -- 23: Attendance status (national single-digit 2-7; passthrough trimmed)
        nullif(trim(attendance_status), '')     as attendance_status_code,
        -- 24: Service discharge date
        {{ parse_uk_date('coalesce(service_discharge_date, discharge_date, date_dischargedfrom_caseload, referral_closure_date)') }}
                                                as discharge_date,

        -- 25: Provider (cleaned ODS code; provider col sparse, fall back to meta)
        provider_codes.cleaned_provider_code    as provider_code,
        -- 26: Service reporting line (coalesce siblings)
        nullif(trim(coalesce(service_reporting_line, service_report_line)), '')
                                                as service_reporting_line,
        -- 27: Service POD
        service_pod                             as service_pod,
        -- 28: Service request identifier (coalesce siblings)
        coalesce(service_request_identifier, service_request_id)
                                                as service_request_id,

        -- Additional well-populated fields from the full (Current) SLAM spec,
        -- trimmed from the proposed 28 but useful and reasonably filled.
        coalesce(site_code_of_treatment, site_code)
                                                as site_of_treatment_code,
        community_care_contact_identifier       as community_care_contact_identifier,
        try_to_number(coalesce(
            clinical_contact_duration_of_care_contact,
            clinical_contact_duration_of_care_activity,
            duration_of_contact
        ))                                      as contact_duration_minutes,

        -- Reporting month parsed from the submission file name (last-resort
        -- period source when neither stated nor contact date is available)
        {{ period_from_file_name('registry.original_file_name') }}
                                                as file_name_period,

        -- Raw period values retained for traceability
        coalesce(dlp_financial_year, financial_year)    as financial_year_raw,
        coalesce(dlp_financial_month, financial_month)  as financial_month_raw

    from {{ ref('raw_sdl_wnl_comopl') }}
    left join provider_codes
        on equal_null(
            upper(trim(coalesce(organisation_identifier_code_of_provider, provider_code, meta_provider_code))),
            provider_codes.source_provider_code
        )
    left join registry
        on registry.file_id = meta_file_id
       and registry.batch_id = meta_batch_id
)

-- Final projection. Derived reporting period sits up front (after the meta
-- keys). Period precedence: provider-stated value -> contact date -> file-name
-- period (each gated to a plausible range so junk cannot create phantom
-- periods). dv_financial_period_source records which was used.
select
    -- META keys
    meta_sk_row_id, meta_file_id, meta_row_id, meta_batch_id,
    meta_partition_date, meta_provider_code, meta_recipient_code, meta_version_id,

    -- Reporting period (derived: stated -> contact date -> file name)
    {{ community_pld_financial_period('contact_date', 'contact_date') }},

    -- DLP standard fields (flag, commissioner, baseline month)
    dlp_flex_or_freeze, dlp_commissioner_code, dlp_baseline_financial_month,

    -- Spec body (fields 6-28, in spec order). dv_referral_received_at /
    -- dv_contact_at combine the spec date + time into a single timestamp.
    local_patient_id, sk_patient_id, dv_year_of_birth, partial_postcode, lsoa,
    gender_code, ethnic_category_code, gp_practice_code, source_of_referral_code,
    referral_received_date, referral_received_time,
    {{ combine_date_time('referral_received_date', 'referral_received_time') }}
                                                as dv_referral_received_at,
    team_type_code, priority_type_code,
    contact_date, contact_time,
    {{ combine_date_time('contact_date', 'contact_time') }}
                                                as dv_contact_at,
    contact_cancellation_reason,
    consultation_type_code, consultation_mechanism_code, attendance_status_code,
    discharge_date, provider_code, service_reporting_line, service_pod,
    service_request_id,

    -- Additional useful fields (full Current spec)
    site_of_treatment_code, community_care_contact_identifier,
    contact_duration_minutes,

    -- Raw period (traceability)
    financial_year_raw, financial_month_raw
from prep
