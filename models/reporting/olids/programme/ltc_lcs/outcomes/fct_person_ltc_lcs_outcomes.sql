{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['ltc_lcs', 'outcomes'])
}}

-- LTC LCS outcomes, one row per person eligible for at least one outcome measure.
-- Each measure adds an is_<measure>_eligible flag and its result columns, null when not eligible.
with htn_rolling as (
    select * from {{ ref('fct_person_ltc_lcs_outcomes_htn_bp_control') }}
),
htn_fy as (
    select * from {{ ref('fct_person_ltc_lcs_outcomes_htn_bp_control_fy') }}
),
people as (
    select person_id from htn_rolling
    union
    select person_id from htn_fy
)
select
    p.person_id,
    -- ICS_HTN_21 denominator; the same for both windows.
    htn_rolling.person_id is not null as is_htn_bp_control_eligible,
    htn_rolling.is_bp_controlled as htn_bp_controlled_rolling,
    htn_rolling.has_bp_in_window as htn_bp_control_rolling_has_reading,
    htn_rolling.latest_bp_date as htn_bp_control_rolling_bp_date,
    htn_rolling.latest_systolic_value as htn_bp_control_rolling_systolic,
    htn_rolling.latest_diastolic_value as htn_bp_control_rolling_diastolic,
    htn_fy.is_bp_controlled as htn_bp_controlled_fy,
    htn_fy.has_bp_in_window as htn_bp_control_fy_has_reading,
    htn_fy.latest_bp_date as htn_bp_control_fy_bp_date,
    htn_fy.latest_systolic_value as htn_bp_control_fy_systolic,
    htn_fy.latest_diastolic_value as htn_bp_control_fy_diastolic
from people p
left join htn_rolling on p.person_id = htn_rolling.person_id
left join htn_fy on p.person_id = htn_fy.person_id
