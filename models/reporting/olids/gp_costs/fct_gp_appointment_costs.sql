{{
    config(
        materialized='table',
        tags=['fact', 'gp_costs', 'kpi']
    )
}}

/*
GP appointment unit-cost fact table.

Grain: one row per practice per month per practitioner_role_group.

Scope: built from int_appointment_gp_clean_recent which is restricted
to the rolling last 60 months of appointment data. This matches OLIDS
retention — see int_appointment_gp_clean_recent for the rationale.

Aggregates the recent appointment view to workforce-mix cost totals.
See models/modelling/olids/appointments/int_appointment_gp_clean.yml
for the full cost derivation methodology — this model only aggregates,
it does not transform the underlying cost values.

Two cost totals per group:
- total_cost_gbp_base_prices  : always in 2024/25 real-terms prices,
                                use for cross-year comparisons
- total_cost_gbp_nominal      : in the appointment's own fiscal year
                                prices (GDP deflator adjusted),
                                use for contemporaneous reporting

cost_is_proxy / cost_proxy_source are seed-determined properties of
each practitioner_role_group — included as passthroughs so consumers
can filter WHERE cost_is_proxy = FALSE to restrict to direct PSSRU
figures.

arrs_appointment_count is exposed as a measure rather than a grain
dimension because is_arrs_role is a property of the underlying
role_code, not practitioner_role_group (e.g. the Pharmacist group
spans both ARRS-flagged R9804 Clinical Pharmacists and non-ARRS
R1290 generic Pharmacist).
*/

select
    publisher_organisation_code as practice_code,
    DATE_TRUNC('month', start_date) as report_month,
    practitioner_role_group,
    -- Part of the grain: a handful of analytical role groups span two
    -- SDS groups (e.g. Nurse includes both 'Nurses' and 'Other Direct
    -- Patient Care' via A&E Staff Nurse; Admin similarly spans
    -- 'Data Quality' and 'Other Direct Patient Care'), so including
    -- sds_role_group in the grain keeps the result deterministic.
    sds_role_group,

    -- fiscal_year_start is functionally determined by report_month so
    -- MAX() returns the same value for every row in the group.
    MAX(fiscal_year_start) as fiscal_year_start,

    -- Seed-determined passthroughs (invariant within practitioner_role_group
    -- because they come from the pssru_unit_costs_2025 seed keyed on
    -- practitioner_role_group; ANY_VALUE is safe here)
    ANY_VALUE(cost_is_proxy) as cost_is_proxy,
    ANY_VALUE(cost_proxy_source) as cost_proxy_source,

    -- Volume
    COUNT(*) as appointment_count,
    SUM(CASE WHEN is_attended THEN 1 ELSE 0 END) as attended_count,
    SUM(CASE WHEN is_dna THEN 1 ELSE 0 END) as dna_count,
    SUM(CASE WHEN is_arrs_role THEN 1 ELSE 0 END) as arrs_appointment_count,

    -- Duration (NULLs from untimed / list sessions are skipped by SUM/AVG)
    SUM(duration_minutes) as total_duration_minutes,
    ROUND(AVG(duration_minutes), 1) as avg_duration_minutes,

    -- Cost in PSSRU 2024/25 real-terms prices (cross-year comparable)
    ROUND(SUM(appointment_cost_gbp_base_prices), 2) as total_cost_gbp_base_prices,
    ROUND(AVG(appointment_cost_gbp_base_prices), 2) as avg_cost_per_appointment_gbp_base_prices,

    -- Cost in the appointment's own fiscal year prices (GDP deflator adjusted)
    ROUND(SUM(appointment_cost_gbp_nominal), 2) as total_cost_gbp_nominal,
    ROUND(AVG(appointment_cost_gbp_nominal), 2) as avg_cost_per_appointment_gbp_nominal,

    -- £ per minute of clinician time (nominal)
    ROUND(
        SUM(appointment_cost_gbp_nominal) / NULLIF(SUM(duration_minutes), 0),
        2
    ) as cost_per_minute_gbp_nominal

from {{ ref('int_appointment_gp_clinical_recent') }}
group by
    publisher_organisation_code,
    DATE_TRUNC('month', start_date),
    practitioner_role_group,
    sds_role_group
