{{ config(materialized='table') }}

-- CKD_64 helper: one row per eligible, registered, living person aged at least 17.
-- The base excludes eGFR codes strictly within one year. Exclude CKD_61/62/63 helpers.
-- EMIS selects AKI within three years, at-risk conditions ever, or specified medication
-- issued strictly within six months before testing the two haematuria routes.
-- Latest microscopic haematuria or red-cell microscopy requires no negative urine
-- blood test or red-cell microscopy on or after its date. Latest microscopic haematuria
-- diagnosis requires exact-code UACR > 30 on or after its date. Other arms have no linked test.
-- The BNF substitute includes lithium carbonate and citrate only.

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
    from ({{ get_ltc_lcs_observations("4fad1128-518d-aab7-8bb8-ed51bd35c7b6", emis_local_exact=true) }})
    where clinical_effective_date >= dateadd(year, -3, current_date())
),

-- Arm B: at-risk conditions (vs2), ever recorded
arm_conditions as (
    select person_id
    from ({{ get_ltc_lcs_observations("a30e0c52-efd9-9ce9-c56a-6d1469fc64d4", emis_local_exact=true) }})
),

-- Arm C: nephrotoxic medication in the last 6 months
arm_medication as (
    -- vs4 Lithium Carbonate/Citrate + vs6 Sulfasalazine/Tacrolimus (expanded)
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("7d11f002-bdf4-e22d-4aa4-5e1de14bb3af,67b57e4a-b4c4-f213-726c-bcead7df9e1a") }})
    where order_date > dateadd(month, -6, current_date())
    union
    -- Unexpanded Lithium Salts and lithium carbonate brand use lithium-only BNF codes.
    select distinct person_id
    from ({{ get_medication_orders(bnf_code='0402030') }})
    where order_date > dateadd(month, -6, current_date())
        and substr(bnf_code, 1, 9) in ('0402030K0', '0402030P0')
),

-- The two haematuria pools have independent Latest 1 restrictions.
latest_haematuria as (
    select person_id, clinical_effective_date, valueset_id
    from ({{ get_ltc_lcs_observations("37f2f047-d4b2-bf56-5c7b-2a9788936448,5bf47b55-ccf0-9e25-d7d1-0a7f4b5f3dac", emis_local_exact=true) }})
    qualify row_number() over (
        partition by person_id, valueset_id
        order by clinical_effective_date desc nulls last, observation_id desc
    ) = 1
),

haematuria_cancellation as (
    select person_id, clinical_effective_date
    from ({{ get_ltc_lcs_observations("63bf0d9b-589c-905a-abd2-e59190e02053", emis_local_exact=true) }})
),

uacr_confirmation as (
    select person_id, clinical_effective_date, result_value
    from ({{ get_ltc_lcs_observations("3b7a55b7-b9a6-f23b-60d6-1015949622d2", match_paths='source') }})
),

arm_haematuria as (
    select h.person_id
    from latest_haematuria as h
    where h.valueset_id = '37f2f047-d4b2-bf56-5c7b-2a9788936448'
        -- A microscopy record belongs to both pools and cancels itself on its date.
        and not exists (
            select 1 from haematuria_cancellation as c
            where c.person_id = h.person_id
                and c.clinical_effective_date::date >= h.clinical_effective_date::date
        )
    union
    select h.person_id
    from latest_haematuria as h
    where h.valueset_id = '5bf47b55-ccf0-9e25-d7d1-0a7f4b5f3dac'
        and exists (
            select 1 from uacr_confirmation as u
            where u.person_id = h.person_id
                and u.clinical_effective_date::date >= h.clinical_effective_date::date
                and u.result_value > 30
        )
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
