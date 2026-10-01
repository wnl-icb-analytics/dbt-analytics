{{ config(materialized='table') }}

-- LTC LCS CKD case finding: ICB_CF_CKD_61_BASE population (shared parent for 61/62/63)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/ckd/icb-cf-ckd-61-1d.md
--
-- ICB_CF_CKD_61_BASE = currently registered, age at least 17, NOT in ICS_METABOLIC_LTC
--   (has_metabolic_excluding_condition flag), AND NOT carrying any CKD-stage code
--   (eligible_for_ckd_case_finding_vs1, 162 codes -> "include patients who do not match").
-- CKD_61 BASE does NOT exclude CF_NHSHC2Y (per review).
--
-- Caveat: canon also excludes two EMIS library items (3de35e4f-... and c913f5a7-...)
--   whose logic is not in the XML export. They are not resolvable here so are omitted;
--   the ICS_METABOLIC_LTC + CKD-stage-code exclusions cover the documented intent.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where age_at_least >= 17
        and has_metabolic_excluding_condition = false
),

-- CKD-stage carriers: exclude (162-code stage 3/4/5 + related set)
ckd_stage_coded as (
    select person_id
    from ({{ get_ltc_lcs_observations("eligible_for_ckd_case_finding_vs1") }})
)

select distinct b.person_id
from base as b
where b.person_id not in (select person_id from ckd_stage_coded)
