{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['ltc_lcs', 'outcomes', 'hypertension'])
}}

-- LTC LCS Outcomes: Hypertension good blood pressure control - ROLLING (default).
-- Window: last known paired BP after today minus 12 months, up to today (EMIS ICS_HTN_21
-- "date > today - 12 months"). This is the "current position" view.
-- See macro get_ltc_lcs_htn_bp_control for the denominator exclusions and thresholds.

{{ get_ltc_lcs_htn_bp_control(
    window_start="dateadd(day, 1, dateadd(month, -12, current_date()))",
    window_end="current_date()"
) }}
