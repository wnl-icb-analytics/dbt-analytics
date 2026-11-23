{{ config(materialized='table') }}

-- CVD_63/64/65 shared cohort: one row per registered living person aged 40 to <84.
-- Requires latest QRISK >=10, without a date window, and no shared metabolic exclusion.
-- Excludes persistent statin adverse reactions at any date and contraindication,
-- statin not indicated or not tolerated events on or before today. Local adverse codes use the shared equivalence rule.
-- This parent permits statin treatment and recent NHS health checks.
-- Excludes QOF palliative care register membership (EMIS library ea06414e).
-- XML library exclusion 3de35e4f lacks a complete exported definition.
-- Authority: EMIS R5 ICB_CF_CVD_63_BASE, structured filters and tests.

with base as (
    select person_id, age_at_least
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where has_metabolic_excluding_condition = false
        and is_on_palliative_care_register = false
),

age_eligible as (
    select person_id
    from base
    where age_at_least >= 40
        and age_at_least < 84
),

statin_adverse as (
    select person_id
    from ({{ get_ltc_lcs_observations("a06baf8e-f9c8-30ab-cee1-c2ba3af35902") }})
),

statin_contra as (
    select person_id
    from ({{ get_ltc_lcs_observations("cd58c9a2-0e17-f7fe-30d3-effd2a7ec9aa") }})
    where clinical_effective_date <= current_date()
),

qrisk_ge10 as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("bc214a50-595d-7b11-b391-6f3d8ef2aa96") }})
    where result_value >= 10
)

select distinct ae.person_id
from age_eligible as ae
inner join qrisk_ge10 as q
    on ae.person_id = q.person_id
where ae.person_id not in (select person_id from statin_adverse)
    and ae.person_id not in (select person_id from statin_contra)
