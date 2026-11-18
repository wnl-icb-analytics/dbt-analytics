{{ config(materialized='table', tags=['monthly-full', 'nice-history'],
    cluster_by=['reporting_date', 'person_id']) }}

{{ nice_alcohol_union('by_month') }}
