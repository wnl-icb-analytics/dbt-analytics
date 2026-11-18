{{ config(materialized='table', tags=['monthly-full', 'nice-history'], cluster_by=['reporting_date', 'person_id']) }}

{{ calculate_nice_multimorbidity_categories('by_month') }}
