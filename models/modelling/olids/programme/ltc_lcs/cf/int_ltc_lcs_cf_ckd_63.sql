{{ config(materialized='table') }}

-- LTC LCS CKD case finding indicator CKD_63: latest UACR > 70, not on CKD register
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/ckd/icb-cf-ckd-63-1d.md (+ -63-1n.md)
-- ICB_CF_CKD_63 = ICB_CF_CKD_63_1D_woEX (61_BASE minus 61_woEX minus 62_woEX, latest UACR > 70), then:
--   Rule 1 (1D, exclude if matched): "Chronic kidney disease screening" code within the
--     last 3 years. Rule 1 (1N): on the CKD register. Both applied.

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_63_woEX') }}
),

-- Rule 1 (1D): CKD screening recorded in the last 3 years (1-code marker)
ckd_screened as (
    select person_id
    from ({{ get_ltc_lcs_observations("with_most_recent_uacr70_and_not_on_ckd_regckd_diagnosi_vs1") }})
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
