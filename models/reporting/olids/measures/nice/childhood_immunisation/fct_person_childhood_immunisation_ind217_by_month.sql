{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND217: https://www.nice.org.uk/indicators/ind217
-- A 4-in-1 preschool booster and at least two MMR doses between the first and fifth birthdays for children who reached 5 in the preceding 12 months.
{{ nice_ind217('by_month') }}
