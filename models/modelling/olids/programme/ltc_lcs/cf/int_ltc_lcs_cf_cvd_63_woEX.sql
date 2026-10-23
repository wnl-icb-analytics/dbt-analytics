{{ config(materialized='table') }}

-- CVD_63 before review-decline exclusion: one row per person in CVD_63_BASE.
-- Requires latest non-HDL >2.5 mmol/L, without a date window, and excludes CVD_61/62_woEX.
-- Non-HDL codes use the shared equivalence rule before selecting the latest result.
-- Dated non-HDL records take precedence over undated records.
-- Requires an explicit statin-product issue, STAT_COD issue or STAT_COD clinical event.
-- Selects the latest record on or before today in each arm, then requires a date strictly after 12 months ago.
-- Authority: EMIS R5 ICB_CF_CVD_63_woEX, structured filters and tests.

-- XML date caps apply before Latest 1, so future records cannot hide earlier records.
with statin_product_issue_at_baseline as (
    select person_id, order_date
    from ({{ get_ltc_lcs_medication_orders("ebb22ff5-788c-4989-66a7-aaf7a68ebb62", include_date_recorded=true) }})
    where order_date <= current_date()
    qualify row_number() over (
        partition by person_id, valueset_id
        order by order_date desc nulls last, date_recorded desc nulls last, medication_order_id desc
    ) = 1
),

statin_code_event_at_baseline as (
    select person_id, clinical_effective_date
    from ({{ get_ltc_lcs_observations("624085a5-e1b3-e962-99f1-e7a6270a20e5", include_date_recorded=true) }})
    where clinical_effective_date <= current_date()
    qualify row_number() over (
        partition by person_id, valueset_id
        order by clinical_effective_date desc nulls last, date_recorded desc nulls last, observation_id desc
    ) = 1
),

statin_code_issue_at_baseline as (
    select person_id, order_date
    from ({{ get_ltc_lcs_medication_orders("624085a5-e1b3-e962-99f1-e7a6270a20e5", include_date_recorded=true) }})
    where order_date <= current_date()
    qualify row_number() over (
        partition by person_id, valueset_id
        order by order_date desc nulls last, date_recorded desc nulls last, medication_order_id desc
    ) = 1
),

base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_cvd_63_base') }}
),

cvd_61_woex as (
    select person_id from {{ ref('int_ltc_lcs_cf_cvd_61_woEX') }}
),

cvd_62_woex as (
    select person_id from {{ ref('int_ltc_lcs_cf_cvd_62_woEX') }}
),

latest_non_hdl as (
    select person_id, result_value
    from ({{ get_ltc_lcs_observations("23f72268-d2d1-6fb0-9fe5-19eeb497790e", include_date_recorded=true) }})
    qualify row_number() over (
        partition by person_id, valueset_id
        order by clinical_effective_date desc nulls last, date_recorded desc nulls last, observation_id desc
    ) = 1
),

non_hdl_gt25 as (
    select person_id
    from latest_non_hdl
    where result_value > 2.5
),

on_statin as (
    select person_id
    from statin_product_issue_at_baseline
    where order_date > dateadd(month, -12, current_date())
    union
    select person_id
    from statin_code_event_at_baseline
    where clinical_effective_date > dateadd(month, -12, current_date())
    union
    select person_id
    from statin_code_issue_at_baseline
    where order_date > dateadd(month, -12, current_date())
)

select distinct b.person_id
from base as b
inner join non_hdl_gt25 as n
    on b.person_id = n.person_id
inner join on_statin as s
    on b.person_id = s.person_id
where b.person_id not in (select person_id from cvd_61_woex)
    and b.person_id not in (select person_id from cvd_62_woex)
