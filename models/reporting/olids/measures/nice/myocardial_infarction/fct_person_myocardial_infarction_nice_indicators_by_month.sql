{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

{{ nice_myocardial_infarction_union('by_month') }}
