{{ config(materialized='table') }}

-- LTC LCS CVD case finding: ICB_CF_CVD_63_BASE population (shared parent for 63 + 64 + 65)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/cvd/icb-cf-cvd-63.md
--
-- CVD_63_BASE = currently registered, age 40 to <84, EXCLUDE who match any of:
--   - ICS_METABOLIC_LTC (has_metabolic_excluding_condition)
--   - statin adverse reaction (vs1, 52 codes)             -- ever
--   - statin contraindicated / not indicated (vs2, TXSTAT_COD, 3 codes) -- ever
--   (library items 3de35e4f / ea06414e not in export -> not implemented)
--   Then REQUIRE QRISK refset (999011011000230107) latest numeric value >= 10 (vs3, CVDASS2_COD).
--
-- Unlike CVD_61_BASE this BASE does NOT exclude CF_NHSHC2Y and does NOT exclude statin users
-- (statin status is the discriminator applied per indicator in the _woEX layer).

with base as (
    select person_id, age_at_least
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where has_metabolic_excluding_condition = false
),

age_eligible as (
    select person_id
    from base
    where age_at_least >= 40
        and age_at_least < 84
),

-- statin adverse reaction (vs1) -- ever
statin_adverse as (
    select person_id
    from ({{ get_ltc_lcs_observations("people_with_qrisk_10_vs1") }})
),

-- statin contraindicated / not indicated (vs2 TXSTAT_COD) -- ever
statin_contra as (
    select person_id
    from ({{ get_ltc_lcs_observations("people_with_qrisk_10_vs2") }})
),

-- QRISK refset latest numeric value >= 10 (vs3 CVDASS2_COD)
qrisk_ge10 as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("people_with_qrisk_10_vs3") }})
    where result_value >= 10
)

select distinct ae.person_id
from age_eligible as ae
inner join qrisk_ge10 as q
    on ae.person_id = q.person_id
where ae.person_id not in (select person_id from statin_adverse)
    and ae.person_id not in (select person_id from statin_contra)
