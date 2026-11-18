{{ config(materialized='table' , cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

{{ nice_fh_assessment_union('by_month') }}
