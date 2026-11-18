{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND216: https://www.nice.org.uk/indicators/ind216
-- At least one MMR dose between 12 and 18 months of age for children who reached 18 months in the preceding 12 months.
{{ nice_ind216('by_month') }}
