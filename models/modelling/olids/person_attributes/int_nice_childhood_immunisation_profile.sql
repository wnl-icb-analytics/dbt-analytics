{{ config(materialized='table', cluster_by=['person_id']) }}
{{ calculate_nice_childhood_immunisation_profile('current') }}
