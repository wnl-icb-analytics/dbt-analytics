{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}
{{ calculate_nice_childhood_immunisation_profile('by_month') }}
