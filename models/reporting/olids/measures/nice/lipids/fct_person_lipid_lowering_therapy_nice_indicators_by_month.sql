{{ config(materialized='table', tags=['monthly-full', 'nice-history'], cluster_by=['reporting_date', 'person_id']) }}

{{ nice_lipids_union('by_month') }}
