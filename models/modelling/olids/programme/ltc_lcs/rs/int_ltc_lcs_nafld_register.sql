-- LTC LCS: NAFLD register
-- EMIS source: 'LTC LCS: NAFLD Register v2*'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/nafld/ltc_lcs_nafld_register_v2.md)
--
-- One row per currently registered, living, non-test person with any exported
-- NAFLD v2 diagnosis code ever recorded. No age or recency restriction.
-- Use the two XML code lists rather than the broader SLD_DX_CODES cluster.
-- Public and equivalent local roots retain mapped concepts and requested descendants.
-- Exclude alcoholic steatosis and alcoholic steatohepatitis records and their descendants, as in the XML.

with
nafld_exception_roots as (
    select translated_to_snomed_code as concept_code
    from {{ ref('stg_reference_ltc_lcs_exceptions') }}
    where valueset_id = '08654d02-552a-7784-076c-e7fcc941d284'
),

nafld_exceptions as (
    select concept_code from nafld_exception_roots
    union
    select tc.sub_type_id::varchar as concept_code
    from {{ ref('stg_nhsd_snomed_sct_transitive_closure') }} as tc
    inner join nafld_exception_roots as ex
        on tc.super_type_id::varchar = ex.concept_code
),

nafld_codes as (
    select distinct obs.person_id
    from ({{ get_ltc_lcs_observations("08654d02-552a-7784-076c-e7fcc941d284,f0bfa8e2-972c-1cba-326f-573ef42e6124") }}) as obs
    where not exists (
        select 1 from nafld_exceptions as ex
        where ex.concept_code = obs.concept_code
    )
),

active as (
    select person_id from {{ ref('dim_person_active_patients') }}
)

select distinct n.person_id
from nafld_codes n
inner join active a on n.person_id = a.person_id
