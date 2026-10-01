{{ config(materialized='table') }}

-- LTC LCS CYP asthma case finding: ICB_CF_CYPAST_61_woEX eligible population (shared parent)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/asthma_cyp/icb-cf-cypast-61.md
--
-- The pre-Rule-1 eligible population. RESPIRATORY case-finding domain (NOT metabolic):
--   ICB_CF_CYPAST_61_BASE: currently registered, age 18 months to 17 years inclusive,
--     NOT on a respiratory register (ICS_RESP_LTC = COPD/asthma/CYP-asthma), and whose
--     latest AST_COD event is NOT an asthma-resolved code. No CF_NHSHC2Y exclusion and no
--     metabolic exclusion (canon _BASE lists neither).
--   ICB_CF_CYPAST_61_woEX: then keep only patients with asthma medication/symptom activity
--     in the last 12 months (any arm):
--       vs1 asthma med products (SCT_PREP, 469 codes, expanded -> valueset id pinned [collision])
--       vs2 drug groups Bronchodilators/Corticosteroids For Inhalation/Drugs For Prophylaxis
--           Of Asthma +5 (dm+d drug group, UNEXPANDED 0/8 -> BNF chapter 03 respiratory)
--       vs3/vs4/vs5 prednisolone (SCT, expanded -> valuesets)
--       vs6 montelukast (SCT, expanded -> valueset)
--       vs7 Suspected asthma (clinical code, last 12 months)
--       vs8 Viral induced wheeze (clinical code, last 12 months; NO age>=6 floor per canon)
--
-- COLLISION: asthma_casefinding_eligible_patients_vs1 has two copies -- the woEX med-product
--   set (id 54940f0f, 469 codes) used here, and the headline Rule-1 marker (id 0bda5482,
--   'Respiratory disease screening', 1 code) used in int_ltc_lcs_cf_cyp_ast_61. Pin ids.
-- EMIS verification flag: canon _BASE also references library item ee5b135f-... whose logic
--   is not in the XML export -- not implemented here, verify in EMIS before final sign-off.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where has_respiratory_excluding_condition = false
        -- age 18 months to 17 years inclusive (17 inclusive = under 18)
        and age_months >= 18
        and age_at_least < 18
),

-- _BASE carve (canon: "Exclude patients who match [ASTRES_COD UNION AST_COD] then Latest 1
-- where SNOMED code IN AST_COD"). AST_COD (be433490, active asthma codes) and ASTRES_COD
-- (e49a6f96, 'Asthma resolved') are DISJOINT. Take each patient's latest asthma event across
-- the UNION of both sets; REJECT when that latest event is an active AST_COD code (current
-- asthma activity). Keep patients whose latest asthma event is 'resolved', or who have none.
ast_union_events as (
    select person_id, observation_id, clinical_effective_date, valueset_id
    from ({{ get_ltc_lcs_observations("e49a6f96-d7a8-ad43-7448-2166a2858204, be433490-3609-4849-20b6-045ddfbdc032") }})
),

latest_ast_event as (
    select person_id, valueset_id
    from ast_union_events
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc, observation_id desc
    ) = 1
),

active_asthma_latest as (
    -- latest asthma event is an active AST_COD code -> reject from the eligible base
    select person_id
    from latest_ast_event
    where valueset_id = 'be433490-3609-4849-20b6-045ddfbdc032'
),

base_carved as (
    select b.person_id
    from base as b
    where b.person_id not in (select person_id from active_asthma_latest)
),

-- woEX eligible arms (any match in the relevant window)
eligible as (
    -- vs1 asthma med products (last 12 months) [collision -> woEX id 54940f0f]
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("54940f0f-b140-2393-9d8d-0104b3103367") }})
    where order_date >= dateadd(month, -12, current_date())
    union
    -- vs2 drug groups (UNEXPANDED 0/8) recovered via BNF chapter 03 (respiratory system) (last 12 months)
    select distinct person_id
    from ({{ get_medication_orders(bnf_code='03') }})
    where order_date >= dateadd(month, -12, current_date())
    union
    -- vs3/vs4/vs5 prednisolone (last 12 months / "last 1 year")
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("asthma_casefinding_eligible_patients_vs3, asthma_casefinding_eligible_patients_vs4, asthma_casefinding_eligible_patients_vs5") }})
    where order_date >= dateadd(month, -12, current_date())
    union
    -- vs6 montelukast (last 12 months / "last 1 year")
    select person_id
    from ({{ get_ltc_lcs_medication_orders_latest("asthma_casefinding_eligible_patients_vs6") }})
    where order_date >= dateadd(month, -12, current_date())
    union
    -- vs7 Suspected asthma (clinical code, last 12 months)
    select person_id
    from ({{ get_ltc_lcs_observations("asthma_casefinding_eligible_patients_vs7") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
    union
    -- vs8 Viral induced wheeze (clinical code, last 12 months; NO age>=6 gate per canon)
    select person_id
    from ({{ get_ltc_lcs_observations("asthma_casefinding_eligible_patients_vs8") }})
    where clinical_effective_date >= dateadd(month, -12, current_date())
)

select distinct bc.person_id
from base_carved as bc
inner join eligible as e
    on bc.person_id = e.person_id
