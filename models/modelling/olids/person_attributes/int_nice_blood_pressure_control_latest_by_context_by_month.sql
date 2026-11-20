{{ config(materialized='table', tags=['monthly-full', 'nice-history'], cluster_by=['reporting_date', 'person_id']) }}

{{ calculate_nice_blood_pressure_control_latest_by_context('by_month') }}
