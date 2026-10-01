{{
    config(
        materialized='table',
        tags=['fact', 'gp_costs', 'kpi']
    )
}}

/*
GP DNA cost fact — clinician time and unit cost lost to "Did Not
Attend" appointments.

Grain: one row per practice per month.

Scope: built from int_appointment_gp_clean_recent (rolling last 60
months matching OLIDS retention) filtered to in-scope appointments
(is_attended OR is_dna). Cancelled, rescheduled and other non-attendance
statuses are excluded so the DNA rate denominator matches fct_gp_dna_rate.

Cost methodology is inherited from int_appointment_gp_clean — see that
model for the full PSSRU + GDP-deflator derivation. This fact splits
the in-scope cost into attended vs DNA components and exposes the
DNA share as both an absolute £ figure and a percentage of total cost.

Use this model when you want a "cost of missed appointments" KPI at
practice level. For role-level cost analysis use fct_gp_appointment_costs.
*/

select
    publisher_organisation_code as practice_code,
    DATE_TRUNC('month', start_date) as report_month,
    -- Functionally determined by report_month; MAX() gives the same
    -- value for every row in the group and keeps the GROUP BY honest
    -- (grain is strictly practice x month).
    MAX(fiscal_year_start) as fiscal_year_start,

    -- Volume (in-scope = attended + DNA)
    SUM(CASE WHEN is_attended OR is_dna THEN 1 ELSE 0 END) as appointment_count,
    SUM(CASE WHEN is_attended THEN 1 ELSE 0 END) as attended_count,
    SUM(CASE WHEN is_dna THEN 1 ELSE 0 END) as dna_count,
    ROUND(
        100.0
        * SUM(CASE WHEN is_dna THEN 1 ELSE 0 END)
        / NULLIF(SUM(CASE WHEN is_attended OR is_dna THEN 1 ELSE 0 END), 0),
        1
    ) as dna_rate_pct,

    -- Total cost (in-scope appointments) in 2024/25 real terms
    ROUND(
        SUM(CASE WHEN is_attended OR is_dna THEN appointment_cost_gbp_base_prices END),
        2
    ) as total_cost_gbp_base_prices,
    -- Total cost (in-scope appointments) in contemporaneous prices
    ROUND(
        SUM(CASE WHEN is_attended OR is_dna THEN appointment_cost_gbp_nominal END),
        2
    ) as total_cost_gbp_nominal,

    -- Attended-only cost (the "value-delivering" portion)
    ROUND(
        SUM(CASE WHEN is_attended THEN appointment_cost_gbp_base_prices END),
        2
    ) as attended_cost_gbp_base_prices,
    ROUND(
        SUM(CASE WHEN is_attended THEN appointment_cost_gbp_nominal END),
        2
    ) as attended_cost_gbp_nominal,

    -- DNA-only cost (the "wasted" / opportunity-cost portion)
    ROUND(
        SUM(CASE WHEN is_dna THEN appointment_cost_gbp_base_prices END),
        2
    ) as dna_cost_gbp_base_prices,
    ROUND(
        SUM(CASE WHEN is_dna THEN appointment_cost_gbp_nominal END),
        2
    ) as dna_cost_gbp_nominal,

    -- DNA cost as a percentage of total in-scope cost. Same value
    -- regardless of whether computed from base or nominal — within a
    -- (practice, month) group the deflator is constant.
    ROUND(
        100.0
        * SUM(CASE WHEN is_dna THEN appointment_cost_gbp_base_prices END)
        / NULLIF(
            SUM(CASE WHEN is_attended OR is_dna THEN appointment_cost_gbp_base_prices END),
            0
        ),
        1
    ) as dna_cost_pct_of_total

from {{ ref('int_appointment_gp_clinical_recent') }}
where is_attended = TRUE or is_dna = TRUE
group by
    publisher_organisation_code,
    DATE_TRUNC('month', start_date)
