{{ config(materialized='table', tags=['monthly-full', 'nice-history'],
    cluster_by=['reporting_date', 'person_id']) }}

{{ nice_multimorbidity_union('by_month') }}
