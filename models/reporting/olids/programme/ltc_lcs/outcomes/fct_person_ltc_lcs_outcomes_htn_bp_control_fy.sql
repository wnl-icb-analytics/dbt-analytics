{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['ltc_lcs', 'outcomes', 'hypertension'])
}}

-- LTC LCS Outcomes: Hypertension good blood pressure control - CURRENT FINANCIAL YEAR.
-- Window: latest date-linked BP since 1 April of the current (in-progress) FY, pinned to the
-- FY end (31 March of next year). Matches the QOF-style "FY year-to-date since 1st April"
-- denominator; no future readings exist, so the numerator is effectively YTD.
-- FY bounds are computed inline from current_date() (project convention: month >= 4 = new FY)
-- and passed to the shared get_ltc_lcs_htn_bp_control macro as date expressions.

{% set fy_start = "date_from_parts(case when month(current_date()) >= 4 then year(current_date()) else year(current_date()) - 1 end, 4, 1)" %}
{% set fy_end = "dateadd(day, -1, dateadd(year, 1, " ~ fy_start ~ "))" %}

{{ get_ltc_lcs_htn_bp_control(
    window_start=fy_start,
    window_end=fy_end
) }}
