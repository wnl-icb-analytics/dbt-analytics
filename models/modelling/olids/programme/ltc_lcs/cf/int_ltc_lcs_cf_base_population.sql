{{ config(materialized='table') }}

-- LTC LCS case finding base population (shared spine)
-- One row per currently-registered, living patient, carrying age attributes and the
-- domain exclusion flags each case-finding indicator layers on top of, per its canon _BASE.
--
-- Canon: every ICB_CF_*_BASE starts from "Currently registered patients", then excludes
-- the relevant LTC set:
--   - metabolic CF (AF, CKD, CVD, DM, HF, HTN): ICS_METABOLIC_LTC (+ CF_NHSHC2Y on most)
--   - respiratory CF (CYP asthma):             ICS_RESP_LTC
-- ICS_METABOLIC_LTC / ICS_RESP_LTC membership comes from int_ltc_lcs_cf_exclusions.
-- CF_NHSHC2Y = any completed NHS health check in the last 24 months.
--
-- The exclusions are exposed as FLAGS (not pre-filtered) so each int_ltc_lcs_cf_<cond>_<n>
-- applies exactly the set its canon report requires (e.g. HF_61 and CKD_61/64 do NOT
-- exclude CF_NHSHC2Y; CYP excludes the respiratory set, not the metabolic one).

with current_registered as (
    select distinct person_id
    from {{ ref('dim_person_current_practice') }}
    where registration_end_date is null
),

person_age as (
    select
        person_id,
        age,
        age_at_least,
        age_months,
        is_deceased
    from {{ ref('dim_person_age') }}
),

exclusions as (
    select
        person_id,
        has_metabolic_excluding_condition,
        has_respiratory_excluding_condition
    from {{ ref('int_ltc_lcs_cf_exclusions') }}
),

nhs_health_check_24m as (
    -- CF_NHSHC2Y: completed NHS health check in the last 24 months
    select distinct person_id
    from {{ ref('int_ltc_lcs_nhs_health_checks') }}
    where clinical_effective_date >= dateadd(month, -24, current_date())
)

select
    pop.person_id,
    pa.age,
    pa.age_at_least,
    pa.age_months,
    coalesce(ex.has_metabolic_excluding_condition, false) as has_metabolic_excluding_condition,
    coalesce(ex.has_respiratory_excluding_condition, false) as has_respiratory_excluding_condition,
    (hc.person_id is not null) as had_nhs_health_check_24m
from current_registered as pop
inner join person_age as pa
    on pop.person_id = pa.person_id
left join exclusions as ex
    on pop.person_id = ex.person_id
left join nhs_health_check_24m as hc
    on pop.person_id = hc.person_id
where coalesce(pa.is_deceased, false) = false
