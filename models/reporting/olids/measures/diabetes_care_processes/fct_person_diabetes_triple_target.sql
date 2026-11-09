{{ config(materialized='table', cluster_by=['person_id']) }}

-- National Diabetes Audit treatment targets for current diabetes register members.
{{ calculate_diabetes_triple_target('current') }}
