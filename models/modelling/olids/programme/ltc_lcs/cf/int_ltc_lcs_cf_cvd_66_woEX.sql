{{ config(materialized='table') }}

-- CVD_66 before review-decline exclusion: one row per registered living person aged 75 to <84.
-- Excludes shared metabolic conditions, recent NHS health checks and CVD_61-65_woEX.
-- Rejects STAT_COD issues/events within 12 months and statin decline within five years,
-- selecting the latest record on or before today before applying each recency test.
-- Rejects persistent statin adverse reactions at any date and contraindication,
-- statin not indicated or not tolerated events on or before today. Local adverse codes match exactly.
-- Requires no QRISK event on or after the five-year anniversary, regardless of its value.
-- XML library exclusions 3de35e4f / ea06414e lack complete exported definitions.
-- Authority: EMIS R5 ICB_CF_CVD_66_BASE and ICB_CF_CVD_66_woEX, structured filters and tests.

-- XML date caps apply before Latest 1, so future records cannot hide earlier records.
with statin_code_issue_at_baseline as (
    select person_id, order_date
    from ({{ get_ltc_lcs_medication_orders("6cf0e193-860f-4b76-9d06-09a3037d09eb") }})
    where order_date <= current_date()
    qualify row_number() over (
        partition by person_id, valueset_id
        order by order_date desc, medication_order_id desc
    ) = 1
),

statin_code_event_at_baseline as (
    select person_id, clinical_effective_date
    from ({{ get_ltc_lcs_observations("6cf0e193-860f-4b76-9d06-09a3037d09eb") }})
    where clinical_effective_date <= current_date()
    qualify row_number() over (
        partition by person_id, valueset_id
        order by clinical_effective_date desc, observation_id desc
    ) = 1
),

statin_decline_at_baseline as (
    select person_id, clinical_effective_date
    from ({{ get_ltc_lcs_observations("c2aa42c9-1dc5-ef51-69f1-80caa1d29cd5") }})
    where clinical_effective_date <= current_date()
    qualify row_number() over (
        partition by person_id, valueset_id
        order by clinical_effective_date desc, observation_id desc
    ) = 1
),

base as (
    select person_id, age_at_least
    from {{ ref('int_ltc_lcs_cf_base_population') }}
    where has_metabolic_excluding_condition = false
        and had_nhs_health_check_24m = false
),

age_eligible as (
    select person_id
    from base
    where age_at_least >= 75
        and age_at_least < 84
),

cvd_61_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_61_woEX') }}),

cvd_62_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_62_woEX') }}),

cvd_63_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_63_woEX') }}),

cvd_64_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_64_woEX') }}),

cvd_65_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_65_woEX') }}),

on_statin as (
    select person_id
    from statin_code_issue_at_baseline
    where order_date > dateadd(month, -12, current_date())
    union
    select person_id
    from statin_code_event_at_baseline
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    select person_id
    from ({{ get_ltc_lcs_observations("75140610-d834-c931-9efa-3f47eaf12f5a", emis_local_exact=true) }})
    union
    select person_id
    from ({{ get_ltc_lcs_observations("4b0f96a0-5505-e71e-1db3-09f5f557ce99") }})
    where clinical_effective_date <= current_date()
    union
    select person_id
    from statin_decline_at_baseline
    where clinical_effective_date > dateadd(year, -5, current_date())
),

qrisk_recorded_5y as (
    select person_id
    from ({{ get_ltc_lcs_observations("a1c6f038-fa5a-8265-93aa-1a1eecf71007") }})
    where clinical_effective_date >= dateadd(year, -5, current_date())
)

select distinct person_id
from age_eligible
where person_id not in (select person_id from on_statin)
    and person_id not in (select person_id from cvd_61_woex)
    and person_id not in (select person_id from cvd_62_woex)
    and person_id not in (select person_id from cvd_63_woex)
    and person_id not in (select person_id from cvd_64_woex)
    and person_id not in (select person_id from cvd_65_woex)
    and person_id not in (select person_id from qrisk_recorded_5y)
