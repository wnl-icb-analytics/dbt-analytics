{{ config(materialized='table') }}

-- CVD_64 before review-decline exclusion: one row per person in CVD_63_BASE.
-- Excludes CVD_61/62/63_woEX. There is no upper QRISK limit.
-- Rejects STAT_COD issues/events within 12 months and statin decline within five years.
-- Selects the latest record on or before today before applying each strict recency test.
-- Authority: EMIS R5 ICB_CF_CVD_64_woEX, structured filters and tests.

-- XML date caps apply before Latest 1, so future records cannot hide earlier records.
with statin_code_issue_at_baseline as (
    select person_id, order_date
    from ({{ get_ltc_lcs_medication_orders("73a1adb2-031d-ffa6-1c8a-e0662ddbc9b4", include_date_recorded=true) }})
    where order_date <= current_date()
    qualify row_number() over (
        partition by person_id, valueset_id
        order by order_date desc nulls last, date_recorded desc nulls last, medication_order_id desc
    ) = 1
),

statin_code_event_at_baseline as (
    select person_id, clinical_effective_date
    from ({{ get_ltc_lcs_observations("73a1adb2-031d-ffa6-1c8a-e0662ddbc9b4", include_date_recorded=true) }})
    where clinical_effective_date <= current_date()
    qualify row_number() over (
        partition by person_id, valueset_id
        order by clinical_effective_date desc nulls last, date_recorded desc nulls last, observation_id desc
    ) = 1
),

statin_decline_at_baseline as (
    select person_id, clinical_effective_date
    from ({{ get_ltc_lcs_observations("4ff4b3d4-c379-00dd-3603-bcdb169589c0", include_date_recorded=true) }})
    where clinical_effective_date <= current_date()
    qualify row_number() over (
        partition by person_id, valueset_id
        order by clinical_effective_date desc nulls last, date_recorded desc nulls last, observation_id desc
    ) = 1
),

base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cvd_63_base') }}
),

cvd_61_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_61_woEX') }}),

cvd_62_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_62_woEX') }}),

cvd_63_woex as (select person_id from {{ ref('int_ltc_lcs_cf_cvd_63_woEX') }}),

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
    from statin_decline_at_baseline
    where clinical_effective_date > dateadd(year, -5, current_date())
)

select distinct b.person_id
from base as b
where b.person_id not in (select person_id from cvd_61_woex)
    and b.person_id not in (select person_id from cvd_62_woex)
    and b.person_id not in (select person_id from cvd_63_woex)
    and b.person_id not in (select person_id from on_statin)
