{{ config(materialized='table') }}

-- LTC LCS CKD case finding indicator CKD_61: latest eGFR < 60, not on CKD register
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/ckd/icb-cf-ckd-61-1d.md (+ -61-1n.md)
-- ICB_CF_CKD_61 = ICB_CF_CKD_61_1D_woEX, then:
--   Rule 1 (1D, exclude if matched): "Chronic kidney disease screening" code (1-code marker,
--     name collides with the 8-code woEX set -> pinned id) within the last 3 years.
--   Rule 1 (1N, exclude if matched): on the LTC LCS CKD Register.
--   Both applied -> net included population.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_61_woEX') }}
),

-- Rule 1 (1D): CKD screening recorded in the last 3 years (1-code marker, pinned id)
ckd_screened as (
    select person_id
    from ({{ get_ltc_lcs_observations("6e168908-5577-8967-f124-d109c66126e1") }})
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
