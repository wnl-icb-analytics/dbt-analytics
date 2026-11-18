{{ config(materialized='table', tags=['monthly-full', 'nice-history'], cluster_by=['reporting_date', 'person_id']) }}

-- Monthly NICE BP control results for IND239 to IND246.
{{ nice_bp_control_union('by_month') }}
