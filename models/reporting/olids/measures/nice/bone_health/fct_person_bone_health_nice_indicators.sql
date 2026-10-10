{{ config(materialized='table') }}

{{ nice_bone_health_union('current') }}
