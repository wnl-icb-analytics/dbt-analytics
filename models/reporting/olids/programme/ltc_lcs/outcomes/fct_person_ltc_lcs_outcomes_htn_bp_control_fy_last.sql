{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['ltc_lcs', 'outcomes', 'hypertension'])
}}

-- LTC LCS Outcomes: Hypertension good blood pressure control - LAST COMPLETED FINANCIAL YEAR.
-- Window: latest date-linked BP within the previous complete FY (1 April -> 31 March).
-- Useful as the fixed end-of-year achievement baseline alongside the rolling/current-FY views.
-- FY bounds are computed inline from current_date() and passed to the shared
-- get_ltc_lcs_htn_bp_control macro as date expressions.

{% set cur_fy_start = "date_from_parts(case when month(current_date()) >= 4 then year(current_date()) else year(current_date()) - 1 end, 4, 1)" %}
{% set last_fy_start = "dateadd(year, -1, " ~ cur_fy_start ~ ")" %}
{% set last_fy_end = "dateadd(day, -1, " ~ cur_fy_start ~ ")" %}

{{ get_ltc_lcs_htn_bp_control(
    window_start=last_fy_start,
    window_end=last_fy_end
) }}
