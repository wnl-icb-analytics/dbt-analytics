{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

{{ nice_bone_health_union('by_month') }}
