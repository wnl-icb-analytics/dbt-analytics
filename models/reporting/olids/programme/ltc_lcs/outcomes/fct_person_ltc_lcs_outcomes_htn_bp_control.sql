{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['ltc_lcs', 'outcomes', 'hypertension'])
}}

-- LTC LCS Outcomes: Hypertension good blood pressure control - ROLLING (default).
-- Window: latest date-linked BP after today minus 12 months, up to today (EMIS R6
-- [ICS_HTN_21 v4_NICE], "date > today - 12 months"). This is the "current position" view.
-- R6 [ICS_HTN_21 v3 L12M] (monitoring only) differs: >= today - 12 months and <= targets.
-- See macro get_ltc_lcs_htn_bp_control for the denominator exclusions and thresholds.

{{ get_ltc_lcs_htn_bp_control(
    window_start="dateadd(day, 1, dateadd(month, -12, current_date()))",
    window_end="current_date()"
) }}
