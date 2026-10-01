{{ config(materialized='table') }}

-- LTC LCS CKD case finding: ICB_CF_CKD_64_1D_woEX population (pre-Rule-1 helper)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/ckd/icb-cf-ckd-64-1d.md
--
-- From ICB_CF_CKD_64_BASE (requires recent eGFR), EXCLUDE everyone in 61_1D_woEX OR
--   62_woEX OR 63_1D_woEX, then include patients matching ANY at-risk arm:
--     A) AKI codes (vs1, 11) within the last 3 years
--     B) at-risk conditions (vs2, 28: BPH, gout, SLE...) - no date window in canon
--     C) nephrotoxic medication issued in the last 6 months:
--          vs3 Lithium Salts (Drug Group, unexpanded -> BNF 0402030)
--          vs4 Lithium Carbonate/Citrate (SCT, expanded -> valueset)
--          vs5 Lithium Carbonate (Essential Pharma) (Brand, unexpanded -> BNF 0402030)
--          vs6 Sulfasalazine, Tacrolimus (SCT, expanded -> valueset)
--     D) microhaematuria: latest record in vs7 (5) or vs9 (4)
--   Canon combines these arms with OR ("include patients who match any of").
--
-- Judgement call: canon lists vs8 (urine tests) + vs10 (UACR) in the haematuria code set
--   but the guide rule text only shows vs7/vs9 "then Latest 1" as the inclusion arms; it
--   does not specify how vs8/vs10 gate (the review describes a confirmation step not present
--   in the export). Implemented as latest-microhaematuria membership (vs7/vs9) to match the
--   documented "Latest 1" rule; vs8/vs10 not used as standalone inclusion.
--
-- Consumed by int_ltc_lcs_cf_ckd_64 (adds Rule 1).

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_64_base') }}
),

ckd_61_woex as (select person_id from {{ ref('int_ltc_lcs_cf_ckd_61_woEX') }}),
ckd_62_woex as (select person_id from {{ ref('int_ltc_lcs_cf_ckd_62_woEX') }}),
ckd_63_woex as (select person_id from {{ ref('int_ltc_lcs_cf_ckd_63_woEX') }}),

-- Arm A: AKI in the last 3 years (vs1 = 11-code woEX set, pinned id)
arm_aki as (
    select person_id
    from ({{ get_ltc_lcs_observations("4fad1128-518d-aab7-8bb8-ed51bd35c7b6") }})
    where clinical_effective_date >= dateadd(year, -3, current_date())
),

-- Arm B: at-risk conditions (vs2), ever recorded
arm_conditions as (
    select person_id
    from ({{ get_ltc_lcs_observations("at_risk_of_ckd_who_are_not_on_ckd_reg_vs2") }})
),

-- Arm C: nephrotoxic medication in the last 6 months
arm_medication as (
    -- vs4 Lithium Carbonate/Citrate + vs6 Sulfasalazine/Tacrolimus (expanded)
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("at_risk_of_ckd_who_are_not_on_ckd_reg_vs4,at_risk_of_ckd_who_are_not_on_ckd_reg_vs6") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    -- vs3 Lithium Salts (drug group) + vs5 Lithium Carbonate brand (unexpanded) -> BNF 0402030 (lithium)
    select distinct person_id
    from ({{ get_medication_orders(bnf_code='0402030') }})
    where order_date >= dateadd(month, -6, current_date())
),

-- Arm D: microhaematuria latest record (vs7 / vs9)
arm_haematuria as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("at_risk_of_ckd_who_are_not_on_ckd_reg_vs7,at_risk_of_ckd_who_are_not_on_ckd_reg_vs9") }})
),

at_risk as (
    select person_id from arm_aki
    union
    select person_id from arm_conditions
    union
    select person_id from arm_medication
    union
    select person_id from arm_haematuria
)

select distinct b.person_id
from base as b
inner join at_risk as r
    on b.person_id = r.person_id
where b.person_id not in (select person_id from ckd_61_woex)
    and b.person_id not in (select person_id from ckd_62_woex)
    and b.person_id not in (select person_id from ckd_63_woex)
