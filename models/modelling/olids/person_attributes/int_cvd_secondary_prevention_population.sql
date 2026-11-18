{{ config(materialized='table', cluster_by=['person_id']) }}

{{ calculate_nice_cvd_secondary_prevention_population('current') }}
