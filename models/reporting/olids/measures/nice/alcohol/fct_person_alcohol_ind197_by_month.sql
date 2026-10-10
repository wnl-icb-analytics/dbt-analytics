{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND197: https://www.nice.org.uk/indicators/ind197
-- Brief intervention within 3 months of any qualifying positive screen (FAST 3 or more, AUDIT-C 5 or more) for people with a first hypertension diagnosis in the preceding 12 months and any recorded positive screen; excludes alcohol-related disorders.
{{ nice_ind197('by_month') }}
