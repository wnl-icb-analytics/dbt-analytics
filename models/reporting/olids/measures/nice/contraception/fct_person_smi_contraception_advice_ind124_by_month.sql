{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

{{ nice_ind124('by_month') }}
