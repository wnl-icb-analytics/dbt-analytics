{{ config(materialized='table') }}

-- LTC LCS HTN case finding: ICB_CF_HTN_61_Base shared population
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/hypertension/icb-cf-htn-61.md
--   (the same _Base is the start population for HTN 61/62/63/65/66)
--
-- Currently registered, living patients, then exclude:
--   - ICS_METABOLIC_LTC                          (has_metabolic_excluding_condition = true)
--   - CF_NHSHC2Y (NHS health check last 24m)     (had_nhs_health_check_24m = true)
--   - Hypertension resolved / White coat HTN     (uclp_priority_groups_base_vs1, 2 codes, ever)
--   - the latest record across HYPRES_COD and HYP_COD, only if that kept record is
--     in HYP_COD. A later resolution defeats an older diagnosis in this rule.
--     The earlier ever-resolved exclusion still applies independently.
--
-- Excludes QOF palliative care register membership (EMIS library ea06414e).
-- Library 3de35e4f lacks a complete exported definition and remains omitted.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where has_metabolic_excluding_condition = false
        and is_on_palliative_care_register = false
        and had_nhs_health_check_24m = false
),

-- Hypertension resolved / White coat hypertension (ever recorded)
hypertension_resolved as (
    select person_id
    from ({{ get_ltc_lcs_observations("uclp_priority_groups_base_vs1") }})
),

-- Rank diagnosis and resolution together, then test the kept record for HYP_COD.
hyp_cod_latest as (
    select person_id
    from ({{ get_ltc_lcs_latest_record_in_set(
        "uclp_priority_groups_base_vs2, uclp_priority_groups_base_vs3",
        "uclp_priority_groups_base_vs3"
    ) }})
)

select distinct b.person_id
from base as b
where b.person_id not in (select person_id from hypertension_resolved)
    and b.person_id not in (select person_id from hyp_cod_latest)
