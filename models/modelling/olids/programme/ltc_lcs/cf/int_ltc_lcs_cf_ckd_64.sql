{{ config(materialized='table') }}

-- LTC LCS CKD case finding indicator CKD_64: at risk of CKD, not on CKD register
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/ckd/icb-cf-ckd-64-1d.md (+ -64-1n.md)
-- ICB_CF_CKD_64 = ICB_CF_CKD_64_1D_woEX (64_BASE minus 61/62/63 woEX, any at-risk arm), then:
--   Rule 1 (1D, exclude if matched): "Chronic kidney disease screening" code (1-code marker,
--     name collides with the 11-code AKI woEX set -> pinned id) within the last 3 years.
--   Rule 1 (1N): on the CKD register. Both applied.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_64_woEX') }}
),

-- Rule 1 (1D): CKD screening recorded in the last 3 years (1-code marker, pinned id)
ckd_screened as (
    select person_id
    from ({{ get_ltc_lcs_observations("7b63b8f0-5846-9636-b5b0-7c2e2f177f30") }})
    where clinical_effective_date >= dateadd(year, -3, current_date())
),

-- Rule 1 (1N): on the CKD register
ckd_register as (
    select person_id
    from {{ ref('fct_person_ckd_register') }}
    where is_on_register = true
)

select distinct person_id
from base_woex
where person_id not in (select person_id from ckd_screened)
    and person_id not in (select person_id from ckd_register)
