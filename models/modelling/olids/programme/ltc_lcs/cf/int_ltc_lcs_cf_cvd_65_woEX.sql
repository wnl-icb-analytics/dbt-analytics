{{ config(materialized='table') }}

-- CVD_65 candidates before the final review-decline exclusion. One row per person.
-- Currently registered, living people aged 40 to under 84 at build date, outside
-- the shared metabolic population and inherited CVD_63_BASE therapy exclusions.
-- Require latest QRISK >= 10, subtract CVD_61-64_woEX and exclude the ever-recorded
-- lipid-lowering therapy exception pool, including decline and maximum tolerated dose.
-- XML criterion 0a123f28 requires MEDICATION_COURSES STATUS C and RX_TYPE U/R/RD:
-- the same statin statement must be active and Automatic, Repeat or Repeat Dispensing.
-- No recent issue or course-start lookback is required. Apply all nested product
-- exceptions with descendants; the warehouse expansion still retains some descendants.
-- The current statin product expansion is incomplete: atorvastatin dm+d products,
-- generic and branded, are missing pending a terminology fix. Membership remains understated.
-- Authority: EMIS R5 updated 27 November 2025, ICB_CF_CVD_65_woEX.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cvd_63_base') }}
),

cvd_61_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_61_woEX') }}),
cvd_62_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_62_woEX') }}),
cvd_63_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_63_woEX') }}),
cvd_64_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_64_woEX') }}),

-- QRISK refset latest >= 10 (vs2, pinned id)
qrisk_ge10 as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("fdead4de-c567-b1c4-15af-7e6a88bfb587") }})
    where result_value >= 10
),

-- The XML pool includes therapy decisions as well as adverse reactions, ever recorded.
lipid_adverse as (
    select person_id
    from ({{ get_ltc_lcs_observations("ed40f2e7-f9f3-1a17-104a-42af018aaf32") }})
),

statin_product_exception_roots as (
    select translated_to_snomed_code as concept_code
    from {{ ref('stg_reference_ltc_lcs_exceptions') }}
    where valueset_id = '21e3808c-e538-c84d-ad57-ed87efd5ef46'
),

statin_product_exceptions as (
    select concept_code from statin_product_exception_roots
    union
    select tc.sub_type_id::varchar as concept_code
    from {{ ref('stg_nhsd_snomed_sct_transitive_closure') }} as tc
    inner join statin_product_exception_roots as ex
        on tc.super_type_id::varchar = ex.concept_code
),

on_statin_course as (
    select person_id
    from ({{ get_ltc_lcs_medication_statements("with_qrisk210_amd_not_on_a_high_intensity_statin_vs3") }}) as ms
    where ms.is_active = true
        and ms.authorisation_type_source_code in ('Automatic', 'Repeat', 'Repeat Dispensing')
        and not exists (
            select 1 from statin_product_exceptions as ex
            where ex.concept_code = ms.concept_code
        )
)

select distinct b.person_id
from base as b
inner join qrisk_ge10 as q
    on b.person_id = q.person_id
inner join on_statin_course as s
    on b.person_id = s.person_id
where b.person_id not in (select person_id from cvd_61_woex)
    and b.person_id not in (select person_id from cvd_62_woex)
    and b.person_id not in (select person_id from cvd_63_woex)
    and b.person_id not in (select person_id from cvd_64_woex)
    and b.person_id not in (select person_id from lipid_adverse)
