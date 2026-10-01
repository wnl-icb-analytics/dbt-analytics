{{ config(materialized='table') }}

-- LTC LCS HTN case finding: ICB_CF_HTN_61_Base shared population
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hypertension/icb-cf-htn-61.md
--   (the same _Base is the start population for HTN 61/62/63/65/66)
--
-- Currently registered, living patients, then exclude:
--   - ICS_METABOLIC_LTC                          (has_metabolic_excluding_condition = true)
--   - CF_NHSHC2Y (NHS health check last 24m)     (had_nhs_health_check_24m = true)
--   - Hypertension resolved / White coat HTN     (uclp_priority_groups_base_vs1, 2 codes, ever)
--   - latest HYP_COD = on-register hypertension  (uclp_priority_groups_base_vs3, 181 codes):
--       canon "Include patients who do NOT match ... Latest 1 where code IN HYP_COD" ->
--       exclude anyone whose latest HYP_COD record exists (largely overlaps the HTN register
--       inside ICS_METABOLIC_LTC, implemented explicitly per canon). vs2 (HYPRES_COD, 2 codes)
--       is a subset of vs1 so the resolved arm is covered by vs1.
--
-- CAVEAT: ICB_CF_HTN_61_Base also references two EMIS library items
--   (3de35e4f-7964-4f24-a0b4-fd42930a1dd1, ea06414e-6bec-4593-837f-5b854c54a8c7) whose logic is
--   NOT in the XML export. Omitted here pending EMIS verification (flagged in canon caveats).

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where has_metabolic_excluding_condition = false
        and had_nhs_health_check_24m = false
),

-- Hypertension resolved / White coat hypertension (ever recorded)
hypertension_resolved as (
    select person_id
    from ({{ get_ltc_lcs_observations("uclp_priority_groups_base_vs1") }})
),

-- Latest HYP_COD (on-register hypertension) exists -> exclude (carve-back per canon)
hyp_cod_latest as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("uclp_priority_groups_base_vs3") }})
)

select distinct b.person_id
from base as b
where b.person_id not in (select person_id from hypertension_resolved)
    and b.person_id not in (select person_id from hyp_cod_latest)
