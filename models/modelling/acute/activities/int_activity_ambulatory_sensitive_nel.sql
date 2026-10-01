{% set icd10_codelist = '1b9d1a9e'%} -- Ambulatory sensitive conditions

with standardised_event_codes AS (
    SELECT DISTINCT 
        codelist_org,
        codelist_slug,
        coding_system,
        {{clean_icd10_code("code")}} as original_event_code,
        left({{clean_icd10_code("code")}}, 3) as concept_code,
        term
    FROM {{ref('stg_reference_opencodelists')}}
    WHERE version_hash  = '{{icd10_codelist}}'
),

base_encounters as (
    select *, left(primary_diagnosis_code, 3) as clipped_primary_diagnosis_code
    from {{ ref('int_sus_apc_encounter') }}
    where left(spell_admission_method, 1) = '2' and spell_admission_method <> '2C' -- Non-elective - emergency and birth of a baby
)


SELECT be.visit_occurrence_id
    , be.sk_patient_id
    , be.source
    , be.organisation_id
    , be.start_date
    , 1 as ambulatory_sensitive_nel, sec.term as primary_diagnosis_term
FROM base_encounters be
INNER JOIN standardised_event_codes sec ON be.clipped_primary_diagnosis_code = sec.concept_code