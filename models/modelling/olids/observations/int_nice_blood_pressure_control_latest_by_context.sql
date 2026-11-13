{{ config(materialized='table', cluster_by=['person_id']) }}

{{ calculate_nice_blood_pressure_control_latest_by_context('current') }}
