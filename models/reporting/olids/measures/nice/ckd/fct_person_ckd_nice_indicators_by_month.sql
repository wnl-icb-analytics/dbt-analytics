{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- Common long-form interface for monthly NICE chronic kidney disease outcomes.
{{ nice_ckd_union('by_month') }}
