{{
    config(
        materialized = 'table',
        tags = ['mhcorl', 'sdl', 'mh']
    )
}}

-- Staging model for the SDL MHCORL feed (provider monthly mental-health
-- activity submissions, originally spreadsheet uploads, remapped via the
-- servicesdatalocal-remapping pipeline).
-- Source: DATA_LAKE__NCL.SDL.MHCORL — 228 cols all TEXT, ~5M rows, 8 providers.
--
-- This stg_ does materially more than typical staging in this repo
-- (gender/ethnicity normalisation, sibling-coalesce across spelling variants,
-- NHS17 mapping, multi-format date parsing, scope filter) because the source
-- is too inconsistent to expose to downstream int_/reporting layers as-is.
-- Materialised as table for query performance over the inline CASE chains.
-- Grain stays 1:1 with source.
--
-- What's done:
--   * Header-text rows ('Financial Year' etc.) filtered out.
--   * Scope: meta_partition_date >= 2022-04-01 (cutoff rationale at WHERE clause).
--   * sk_patient_id = NHS-number-derived hash (matches stg_csds_*, stg_mhsds_*,
--     stg_sus_* convention). The bare SK_PATIENT_ID column is a different
--     identifier — kept as sk_patient_id_local_hash with do-not-join warning.
--   * Sibling spelling-variants coalesced (CONTACT_DATE/CONTACTDATE,
--     team_name/lps_team_name/team, etc.) including provider-specific aliases
--     for FY (financial_year/dlp_financial_year/fin_year) and contact date
--     (col_1_stf2_fcontactdatetime for RV300, col_1st_f2_fcontact_date_time
--     for RKL00).
--   * Multi-format date parsing (UK DD/MM/YYYY, DD-Mon-YY Excel default, ISO,
--     SQL-Server US MM/DD/YYYY HH12:MI:SS AM) via parse_uk_date macro.
--   * Gender canonicalised across 25+ source values (1/2/M/F/Male/Female/...).
--   * Ethnicity_code mapped to NHS Census 2001 17-category labels inline.
--     Free-text ethnicity passed through; RRP00 (90% of volume) supplies only
--     free text so ethnicity_category_nhs17 is NULL there — bucket downstream.
--   * Financial year regex-parsed across 7+ source formats; emitted as both
--     start year and canonical 'YYYY-YY'.
--
-- What's dropped (vs source's 228 cols):
--   * 122 cols >=99% null in source.
--   * Demographics with <10% fill: age, age_of_patient, year_of_birth, age_band,
--     marital_status_code/desc, language.
--   * Provider/site/ward/consultant cols that only one small provider (RNK00,
--     5.5% of volume) populates — analytically unusable for cross-provider
--     rollups; re-derive in a provider-specific intermediate if needed.
--   * Sparse cost/activity cols (cost, unit_cost, in_month_price_actual).
--     (in_month_activity_actual is retained — surfaced on request despite sparse fill.)
--   * Sparse clinical cols (primary_diagnosis 0.8%, cluster 0.1%, etc.).
--   * mhsds_person_id (0.12% — only RWK00 2018 + RRP00 2017).
--   * RNK00/RAT00-only date cols (start_date, date_accepted, etc.) — 0% for
--     the dominant RRP00 feed, so cross-provider use is impossible.
--
-- Coverage caveats (cannot be fixed in this model):
--   * RRP00 (Barnet, Enfield & Haringey MH Trust, ~93% volume), RNK00
--     (Tavistock & Portman), RAT00 (NELFT) are the only providers still
--     submitting from 2024-04 onwards.
--   * RV300 (CNWL) stopped 2023-09; RKL00 (West London NHS Trust) and other
--     smaller providers stopped earlier.
--   * 2021 is missing RRP00 entirely — dq_tier='transitional' flags this.

with src as (
    select
        *,
        coalesce(financial_year, dlp_financial_year, fin_year)  as financial_year_any,
        coalesce(
            contact_date,
            contactdate,
            col_1_stf2_fcontactdatetime,
            col_1st_f2_fcontact_date_time
        )                                                       as contact_date_any,

        -- Resolve a clean NHS Census 2001 EthnicCategoryCode from whatever the
        -- provider supplied: code column, code-in-text, or free-text variant.
        case
            -- code columns (RNK00, RAT00, RWK00 supply these directly)
            when upper(trim(coalesce(ethnicity_code, ethnic_code))) in
                ('A','B','C','D','E','F','G','H','J','K','L','M','N','P','R','S','Z','99','0')
                then upper(trim(coalesce(ethnicity_code, ethnic_code)))

            -- the ethnicity column itself sometimes contains a bare letter code
            when upper(trim(ethnicity)) in
                ('A','B','C','D','E','F','G','H','J','K','L','M','N','P','R','S','Z','99','0')
                then upper(trim(ethnicity))

            -- explicit unknown / not stated
            when upper(trim(ethnicity)) like 'NOT KNOWN%'                          then '99'
            when upper(trim(ethnicity)) in ('UNKNOWN', 'INFORMATION NOT YET OBTAINED') then '99'
            when upper(trim(ethnicity)) like 'NOT STATED%'                         then 'Z'
            when upper(trim(ethnicity)) in ('REFUSED', 'NOT STATED')               then 'Z'

            -- White: British (incl. UK home nations + bare 'British')
            when upper(trim(ethnicity)) in (
                'WHITE - BRITISH', 'WHITE BRITISH', 'BRITISH',
                'WHITE - ENGLISH', 'WHITE - WELSH', 'WHITE - SCOTTISH',
                'WHITE - NORTHERN IRISH', 'WHITE - CORNISH'
            ) then 'A'

            -- White: Irish
            when upper(trim(ethnicity)) in ('WHITE - IRISH', 'WHITE IRISH', 'IRISH') then 'B'

            -- White: Other (Cypriot, Polish, Turkish, Italian, Albanian, Croatian, Serbian,
            -- Kosovan, Greek, Greek/Turkish Cypriot, Other European, former USSR/Yugoslavia,
            -- Gypsy/Roma/Traveller, unspecified White, etc.)
            when upper(trim(ethnicity)) like 'WHITE%'
              or upper(trim(ethnicity)) like 'ANY OTHER WHITE%'
              or upper(trim(ethnicity)) = 'WHITE'
            then 'C'

            -- Mixed: White and Black Caribbean
            when upper(trim(ethnicity)) in (
                'MIXED - WHITE & BLACK CARIBBEAN',
                'MIXED - WHITE AND BLACK CARIBBEAN',
                'WHITE AND BLACK CARIBBEAN',
                'MIXED WHITE AND BLACK CARIBBEAN'
            ) then 'D'

            -- Mixed: White and Black African
            when upper(trim(ethnicity)) in (
                'MIXED - WHITE & BLACK AFRICAN',
                'MIXED - WHITE AND BLACK AFRICAN',
                'WHITE AND BLACK AFRICAN',
                'MIXED WHITE AND BLACK AFRICAN'
            ) then 'E'

            -- Mixed: White and Asian
            when upper(trim(ethnicity)) in (
                'MIXED - WHITE & ASIAN', 'MIXED - WHITE AND ASIAN',
                'WHITE AND ASIAN', 'MIXED WHITE AND ASIAN'
            ) then 'F'

            -- Mixed: Other (catch-all)
            when upper(trim(ethnicity)) like 'MIXED%'
              or upper(trim(ethnicity)) = 'MIXED'
              or upper(trim(ethnicity)) like 'ANY OTHER MIXED%'
            then 'G'

            -- Asian subgroups
            when upper(trim(ethnicity)) in (
                'ASIAN OR ASIAN BRITISH - INDIAN', 'ASIAN/ASIAN BRITISH INDIAN', 'INDIAN'
            ) then 'H'
            when upper(trim(ethnicity)) in (
                'ASIAN OR ASIAN BRITISH - PAKISTANI', 'ASIAN/ASIAN BRITISH PAKISTANI', 'PAKISTANI'
            ) then 'J'
            when upper(trim(ethnicity)) in (
                'ASIAN OR ASIAN BRITISH - BANGLADESHI', 'ASIAN/ASIAN BRITISH BANGLADESHI', 'BANGLADESHI'
            ) then 'K'
            -- Asian: Other (Sri Lankan, Tamil, Punjabi, Kashmiri, Mixed Asian, etc.)
            when upper(trim(ethnicity)) like 'ASIAN%'
              or upper(trim(ethnicity)) = 'ASIAN'
              or upper(trim(ethnicity)) like 'ANY OTHER ASIAN%'
            then 'L'

            -- Black subgroups
            when upper(trim(ethnicity)) in (
                'BLACK OR BLACK BRITISH - CARIBBEAN', 'BLACK/BLACK BRITISH CARIBBEAN', 'CARIBBEAN'
            ) then 'M'
            -- Black: African (incl. Somali, Nigerian — common subgroups treated as African)
            when upper(trim(ethnicity)) in (
                'BLACK OR BLACK BRITISH - AFRICAN', 'BLACK/BLACK BRITISH AFRICAN',
                'BLACK OR BLACK BRITISH - SOMALI', 'BLACK OR BLACK BRITISH - NIGERIAN',
                'AFRICAN'
            ) then 'N'
            -- Black: Other
            when upper(trim(ethnicity)) like 'BLACK%'
              or upper(trim(ethnicity)) = 'BLACK'
              or upper(trim(ethnicity)) like 'ANY OTHER BLACK%'
            then 'P'

            -- Chinese (its own NHS17 bucket)
            when upper(trim(ethnicity)) in ('OTHER ETHNIC GROUPS - CHINESE', 'CHINESE') then 'R'

            -- Other ethnic groups (Iranian, Arab, Kurdish, Latin American, North African,
            -- Filipino, Japanese, Vietnamese, Moroccan, Israeli, Malaysian, etc.)
            when upper(trim(ethnicity)) like 'OTHER%'
              or upper(trim(ethnicity)) = 'OTHER'
              or upper(trim(ethnicity)) like 'ANY OTHER%'
            then 'S'
        end                                                     as eth_letter_code
    from {{ source('sdl', 'MHCORL') }}
    where coalesce(financial_year, '') not in ('Financial Year', '')
       or financial_year is null
),

normalised as (
    select
        -- META keys (from SDL pipeline, fully reliable)
        meta_sk_row_id::number(38,0)            as meta_sk_row_id,
        meta_file_id::number(38,0)              as meta_file_id,
        meta_row_id::number(38,0)               as meta_row_id,
        meta_batch_id::number(38,0)             as meta_batch_id,
        meta_partition_date::date               as meta_partition_date,
        meta_provider_code                      as meta_provider_code,
        meta_recipient_code                     as meta_recipient_code,
        meta_version_id                         as meta_version_id,

        -- Patient identifiers
        {{ consistent_sk_patient_id_format('sk_patient_id_nhs_number') }}  as sk_patient_id,
        sk_patient_id                           as sk_patient_id_local_hash,

        -- Period
        try_to_number(reporting_month)          as reporting_month,
        case
            when financial_year_any is null or financial_year_any = 'Financial Year'
                then null
            when regexp_substr(financial_year_any, '\\d{4}') is not null
                then try_to_number(regexp_substr(financial_year_any, '\\d{4}'))
        end                                     as financial_year_start,
        case
            when financial_year_any is null or financial_year_any = 'Financial Year'
                then null
            when regexp_substr(financial_year_any, '\\d{4}') is not null
                then regexp_substr(financial_year_any, '\\d{4}')
                  || '-'
                  || lpad((try_to_number(regexp_substr(financial_year_any, '\\d{4}')) + 1) % 100, 2, '0')
        end                                     as financial_year_canonical,
        financial_year_any                      as financial_year_raw,

        -- Activity dates (only the two reliable cross-provider dates retained)
        {{ parse_uk_date('coalesce(referral_date, date_of_referral, dateofreferral)') }}
                                                as referral_date,
        coalesce(
            {{ parse_uk_date('contact_date_any') }},
            {{ parse_uk_timestamp('contact_date_any') }}::date
        )                                       as contact_date,

        -- Demographics
        case
            when upper(trim(gender)) in ('1', 'M', 'MALE')                  then 'Male'
            when upper(trim(gender)) in ('2', 'F', 'FEMALE')                then 'Female'
            when upper(trim(gender)) in ('9', 'NOT SPECIFIED', 'NOT STATED')
                                                                            then 'Not specified'
            when upper(trim(gender)) in ('0', 'U', 'X', 'NOT KNOWN', 'UNKNOWN', 'NOT KNOWN (PERSON STATED GENDER CODE NOT RECORDED)')
                                                                            then 'Not known'
            when upper(trim(gender)) like 'INDETERMINATE%'                  then 'Indeterminate'
            when upper(trim(gender)) in ('NON-BINARY', 'OTHER')             then 'Other'
            when gender is null                                             then null
            when upper(trim(gender)) in ('REMOVEDA', 'GENDER')              then null
            else 'Not known'
        end                                     as gender,

        -- Resolve a clean NHS letter code, then look up the 17-cat label via macro.
        -- Provider-specific cleaning lives here; the dictionary lookup is reusable.
        --
        -- Source forms handled:
        --   * letter codes already supplied (RNK00, RAT00, RWK00)
        --   * 'White - X' / 'Asian or Asian British - X' / etc. prefixed (RRP00)
        --   * bare names (British, Indian, Caribbean, ...) — RV300
        -- Aggregations that don't fit a single bucket (BME, NON BME) → no code → NULL.
        {{ nhs_ethnicity_17_label('eth_letter_code') }}
                                                as ethnicity_17,
        case
            when upper(trim(eth_letter_code)) in ('A', 'B', 'C')           then 'White'
            when upper(trim(eth_letter_code)) in ('D', 'E', 'F', 'G')      then 'Mixed'
            when upper(trim(eth_letter_code)) in ('H', 'J', 'K', 'L')      then 'Asian or Asian British'
            when upper(trim(eth_letter_code)) in ('M', 'N', 'P')           then 'Black or Black British'
            when upper(trim(eth_letter_code)) in ('R', 'S')                then 'Other Ethnic Groups'
            when upper(trim(eth_letter_code)) in ('Z', '99', '0')          then 'Unknown'
        end                                     as ethnicity_6,
        dv_lsoa                                 as lsoa,
        dv_partial_post_code                    as partial_post_code,

        -- Organisation (only the cross-provider-reliable cols)
        provider_code                           as provider_code,
        commissioner_code                       as commissioner_code,
        commissioner_name                       as commissioner_name,
        -- ODS GP practice code is 6 chars (1 letter + 5 digits). Source has:
        --   * valid 6-char codes (~99.4%)
        --   * 9-char codes with 3-digit sub-practice/branch suffix (e.g. E83018001) — strip
        --   * '999' placeholder (unknown), 5-char garbage, literal 'entry' — NULL
        case
            when coalesce(gp_practice_code, general_practice_code, gppct_code)
                rlike '^[A-Za-z][0-9]{5}'
            then upper(left(coalesce(gp_practice_code, general_practice_code, gppct_code), 6))
        end                                     as gp_practice_code,
        gp_code                                 as gp_code,
        coalesce(team_name, lps_team_name, lpsteamname, team)
                                                as team_name,
        coalesce(team_code, lps_team_code)      as team_code,

        -- Service / financial categorisation
        -- Standardised to exactly 'Flex' / 'Freeze' (case variants, 'Frozen' and
        -- the DLP PRIMARY/REFRESH terms folded in; '1'/'0' and other values -> NULL).
        coalesce(
            {{ clean_flex_or_freeze('flex_or_freeze') }},
            {{ clean_flex_or_freeze('dlp_flexor_freeze') }}
        )                                       as flex_or_freeze,
        finance_category                        as finance_category,
        costing_code_description                as costing_code_description,

        -- Activity
        coalesce(source_of_referral, referral_source, referralsource)
                                                as source_of_referral,
        consultation_medium                     as consultation_medium,
        coalesce(consultation_type, contact_type)
                                                as contact_type,
        try_to_number(duration_of_contact_minutes)
                                                as duration_of_contact_minutes,
        appointment_sequence_id                 as appointment_sequence_id,
        -- Provider-reported in-month activity count. Sparse (only some providers
        -- populate it) but surfaced on request; cast to numeric, decimals retained.
        try_to_double(in_month_activity_actual) as in_month_activity_actual

    from src
)

select *
from normalised
-- Scope cutoff: FY22/23 onwards. Earlier years have major DQ issues:
--   * 2017 has only 9k rows with no contact_date
--   * 2020-21 is COVID-era with degraded gender/ethnicity/date fill
--   * 2021 is missing RRP00 entirely (90% of normal volume)
-- From 2022-04 onwards every key field is consistently >=87% populated.
where meta_partition_date >= '2022-04-01'
