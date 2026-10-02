{{ config(materialized='table', cluster_by=['person_id']) }}

-- NICE assesses the latest complete pair, including an implausible latest pair.
{{ calculate_nice_blood_pressure_latest('current') }}
