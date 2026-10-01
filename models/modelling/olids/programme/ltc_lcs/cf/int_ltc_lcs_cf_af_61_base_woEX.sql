{{ config(materialized='table') }}

-- LTC LCS AF case finding: ICB_CF_AF_61_base_woEX population (shared parent)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/af/icb-cf-af-61.md
--
-- The pre-Rule-1 AF_61 population. Consumed by both int_ltc_lcs_cf_af_61 (which adds the
-- final Rule 1) and int_ltc_lcs_cf_af_62 (which excludes this whole population).
--
-- ICB_CF_AF_61_BASE: currently registered, NOT in ICS_METABOLIC_LTC, NOT CF_NHSHC2Y,
--   NOT on AF/HF register (both already inside ICS_METABOLIC_LTC), AND on AF-relevant
--   medication in the last 6 months:
--     vs1 Oral Anticoagulants (dm+d drug group, unexpanded -> BNF 0208)
--     vs2 Digoxin/Flecainide/Propafenone (SCT, expanded -> valueset)
--     vs3 Cardiac Glycosides (dm+d drug group, unexpanded -> BNF 020101)
--     vs4 Anticoagulants And Protamine (dm+d drug group, unexpanded -> BNF 0208)
--   The unexpanded drug groups are recovered via BNF sections, mirroring the RS AF model.
-- ICB_CF_AF_61_base_woEX: then exclude the woEX clinical-code sets (no date filter / ever):
--     vs1 (223 codes: antiphospholipid, DVT/PE, thrombotic disorders) [pinned id, name collides]
--     vs2 (8 codes: atrial fibrillation / flutter)
--     vs3 (Long COVID-19)
--     vs4 (Hypoplastic left heart syndrome) — canon "include if NOT match"

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where has_metabolic_excluding_condition = false
        and had_nhs_health_check_24m = false
),

-- AF-relevant medication in the last 6 months (any arm)
af_medication as (
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("patients_on_digoxin_flecainide_propafenone_or_anticoag_vs2") }})
    where order_date >= dateadd(month, -6, current_date())
    union
    -- vs1 Oral Anticoagulants + vs4 Anticoagulants And Protamine (unexpanded drug groups) via BNF 2.8
    select distinct person_id
    from ({{ get_medication_orders(bnf_code='0208') }})
    where order_date >= dateadd(month, -6, current_date())
    union
    -- vs3 Cardiac Glycosides (unexpanded drug group) via BNF 2.1.1
    select distinct person_id
    from ({{ get_medication_orders(bnf_code='020101') }})
    where order_date >= dateadd(month, -6, current_date())
),

-- woEX clinical exclusions (ever recorded)
woex_excluded as (
    -- vs1: 223-code DVT/PE/antiphospholipid set (name collides with the 2-code Rule-1 marker -> pin id)
    select person_id
    from ({{ get_ltc_lcs_observations("f4f774b1-0949-b062-b74a-d03f7d820a81") }})
    union
    -- vs2: atrial fibrillation / flutter (8 codes)
    select person_id
    from ({{ get_ltc_lcs_observations("af_case_finding_eligible_population_on_af_medication_vs2") }})
    union
    -- vs3: Long COVID-19
    select person_id
    from ({{ get_ltc_lcs_observations("af_case_finding_eligible_population_on_af_medication_vs3") }})
    union
    -- vs4: Hypoplastic left heart syndrome (canon: include if NOT match -> exclude if match)
    select person_id
    from ({{ get_ltc_lcs_observations("af_case_finding_eligible_population_on_af_medication_vs4") }})
)

select distinct b.person_id
from base as b
inner join af_medication as m
    on b.person_id = m.person_id
where b.person_id not in (select person_id from woex_excluded)
