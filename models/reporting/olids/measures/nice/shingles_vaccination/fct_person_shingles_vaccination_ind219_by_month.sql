{{ config(materialized='table', tags=['monthly-full', 'nice-history'], cluster_by=['reporting_date', 'person_id']) }}

-- NICE IND219: https://www.nice.org.uk/indicators/ind219
-- Shingles vaccination between the 70th and 75th birthdays for people who reached 75 in the preceding 12 months; excludes immunosuppressed people.
{{ nice_ind219('by_month') }}
