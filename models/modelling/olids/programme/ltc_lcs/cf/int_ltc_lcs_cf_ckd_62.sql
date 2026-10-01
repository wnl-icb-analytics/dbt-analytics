{{ config(materialized='table') }}

-- LTC LCS CKD case finding indicator CKD_62: latest UACR > 4, not on CKD register
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/ckd/icb-cf-ckd-62.md
-- ICB_CF_CKD_62 = ICB_CF_CKD_62_woEX (61_BASE minus 61_woEX, latest UACR > 4), then:
--   Rule 1 (exclude if matched): "Chronic kidney disease screening" code within the last
--     3 years. Plus the CKD register exclusion (consistent with the 1N variants).

with base_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_62_woEX') }}
),

-- Rule 1: CKD screening recorded in the last 3 years (1-code marker)
ckd_screened as (
    select person_id
    from ({{ get_ltc_lcs_observations("with_most_recent_2_uacr4_and_not_on_ckd_regckd_diagnosis_vs1") }})
    where clinical_effective_date >= dateadd(year, -3, current_date())
),

-- on the CKD register
ckd_register as (
    select person_id
    from {{ ref('fct_person_ckd_register') }}
    where is_on_register = true
)

select distinct person_id
from base_woex
where person_id not in (select person_id from ckd_screened)
    and person_id not in (select person_id from ckd_register)
