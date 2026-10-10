{{ config(materialized='table', cluster_by=['person_id']) }}

{{ calculate_nice_bone_therapy('current') }}
