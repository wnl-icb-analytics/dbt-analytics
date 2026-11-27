{{ config(materialized='table') }}

-- CVD_61/62 shared cohort: one row per registered living person aged 40 to <84.
-- Excludes the shared metabolic population and NHS health checks in the last two years.
-- Excludes the latest explicit statin-product issue or STAT_COD event within 12 months,
-- and latest statin decline within five years, selecting records on or before today.
-- Excludes persistent statin adverse reactions at any date and contraindication,
-- statin not indicated or not tolerated events on or before today. Local adverse codes use the shared equivalence rule.
-- Excludes QOF palliative care register membership (EMIS library ea06414e).
-- XML library exclusion 3de35e4f lacks a complete exported definition.
-- Authority: EMIS R5 ICB_CF_CVD_61_BASE, structured filters and tests.

-- XML date caps apply before Latest 1, so future records cannot hide earlier records.
with statin_product_issue_at_baseline as (
    select person_id, order_date
    from ({{ get_ltc_lcs_medication_orders("476ee994-f9a6-21f8-16ac-bcd937a23da5", include_date_recorded=true) }})
    where order_date <= current_date()
    qualify row_number() over (
        partition by person_id, valueset_id
        order by order_date desc nulls last, date_recorded desc nulls last, medication_order_id desc
    ) = 1
),

statin_code_event_at_baseline as (
    select person_id, clinical_effective_date
    from ({{ get_ltc_lcs_observations("64531256-e997-1a46-caf9-45de4bd95bbe", include_date_recorded=true) }})
    where clinical_effective_date <= current_date()
    qualify row_number() over (
        partition by person_id, valueset_id
        order by clinical_effective_date desc nulls last, date_recorded desc nulls last, observation_id desc
    ) = 1
),

statin_decline_at_baseline as (
    select person_id, clinical_effective_date
    from ({{ get_ltc_lcs_observations("49465e07-b9f2-275e-293a-61c00c62a4a0", include_date_recorded=true) }})
    where clinical_effective_date <= current_date()
    qualify row_number() over (
        partition by person_id, valueset_id
        order by clinical_effective_date desc nulls last, date_recorded desc nulls last, observation_id desc
    ) = 1
),

base as (
    select person_id, age_at_least
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where has_metabolic_excluding_condition = false
        and is_on_palliative_care_register = false
        and had_nhs_health_check_24m = false
),

age_eligible as (
    select person_id
    from base
    where age_at_least >= 40
        and age_at_least < 84
),

on_statin_products as (
    select person_id
    from statin_product_issue_at_baseline
    where order_date > dateadd(month, -12, current_date())
),

on_statin_code as (
    select person_id
    from statin_code_event_at_baseline
    where clinical_effective_date > dateadd(month, -12, current_date())
),

statin_adverse as (
    select person_id
    from ({{ get_ltc_lcs_observations("38fdc3b5-7de4-b15c-6577-3f3142bb313e") }})
),

statin_contra as (
    select person_id
    from ({{ get_ltc_lcs_observations("312c880f-3b26-2eef-48ff-7f2795ed33ef") }})
    where clinical_effective_date <= current_date()
),

statin_declined as (
    select person_id
    from statin_decline_at_baseline
    where clinical_effective_date > dateadd(year, -5, current_date())
)

select distinct person_id
from age_eligible
where person_id not in (select person_id from on_statin_products)
    and person_id not in (select person_id from on_statin_code)
    and person_id not in (select person_id from statin_adverse)
    and person_id not in (select person_id from statin_contra)
    and person_id not in (select person_id from statin_declined)
