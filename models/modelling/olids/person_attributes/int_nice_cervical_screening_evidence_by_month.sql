{{ config(materialized='table', tags=['monthly-full', 'nice-history'], cluster_by=['reporting_date', 'person_id']) }}

{{ calculate_nice_cervical_screening_evidence('by_month') }}
