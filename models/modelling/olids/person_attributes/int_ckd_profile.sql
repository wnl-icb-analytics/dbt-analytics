{{ config(cluster_by=['person_id']) }}

-- NICE shared ckd profile at the current reference date.
{{ calculate_nice_ckd_profile('current') }}
