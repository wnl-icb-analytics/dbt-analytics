{{
    config(
        materialized='table',
        cluster_by=['reporting_date', 'person_id'],
        tags=['ltc_lcs', 'outcomes', 'hypertension', 'monthly-full'])
}}

-- LTC LCS Outcomes: Hypertension good blood pressure control at each of the last 60 completed
-- month-ends. Window: latest date-linked BP after the month-end minus 12 months, up to the
-- month-end (EMIS ICS_HTN_21 "date > today - 12 months", run at the month-end).
-- See macro get_ltc_lcs_htn_bp_control for the denominator exclusions and thresholds.

{{ get_ltc_lcs_htn_bp_control(
    window_start="dateadd(day, 1, dateadd(month, -12, d.reporting_date))",
    window_end="d.reporting_date",
    reference='by_month'
) }}
