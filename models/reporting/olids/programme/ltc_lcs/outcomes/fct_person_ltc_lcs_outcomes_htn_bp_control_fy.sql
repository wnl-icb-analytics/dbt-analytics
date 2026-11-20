{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['ltc_lcs', 'outcomes', 'hypertension'])
}}

-- LTC LCS Outcomes: Hypertension good blood pressure control - CURRENT FINANCIAL YEAR.
-- Window: latest date-linked BP from 1 April of the current FY up to today, so future-dated
-- readings are ignored, as in the rolling model. The FY start is computed from current_date()
-- (project convention: month >= 4 = new FY).

{% set fy_start = "date_from_parts(case when month(current_date()) >= 4 then year(current_date()) else year(current_date()) - 1 end, 4, 1)" %}

{{ get_ltc_lcs_htn_bp_control(
    window_start=fy_start,
    window_end="current_date()"
) }}
