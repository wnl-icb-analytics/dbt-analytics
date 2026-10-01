{{ config(materialized='table') }}

-- LTC LCS CKD case finding: ICB_CF_CKD_64_BASE population (parent for 64)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/ckd/icb-cf-ckd-64-1d.md
--
-- ICB_CF_CKD_64_BASE = currently registered, age at least 17, NOT in ICS_METABOLIC_LTC,
--   AND with NO eGFR test recorded in the last 1 year. Canon: "Include patients who do NOT
--   match Clinical Codes with [eGFR codes] where Value > 0 AND Date within the last 1 year
--   OR [eGFR codes] where Date within the last 1 year" (icb-cf-ckd-64-1d.md) -- i.e. case
--   find at-risk patients who have NOT been recently eGFR-tested (anti-join, not inner join).
-- CKD_64 BASE does NOT exclude CF_NHSHC2Y (per review).
--
-- Caveat: two unresolvable EMIS library items (3de35e4f-..., c913f5a7-...) omitted as above.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where age_at_least >= 17
        and has_metabolic_excluding_condition = false
),

-- Require an eGFR code recorded in the last 1 year (canon: Value > 0 within 1y OR any record within 1y)
recent_egfr as (
    select person_id
    from ({{ get_ltc_lcs_observations("ckd_case_finding_vs1") }})
    where clinical_effective_date >= dateadd(year, -1, current_date())
)

select distinct b.person_id
from base as b
where b.person_id not in (select person_id from recent_egfr)
