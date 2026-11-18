{{ config(materialized='table') }}

-- NICE IND218: https://www.nice.org.uk/indicators/ind218
-- At least one MMR dose between the first and fifth birthdays for children who reached 5 in the preceding 12 months.
{{ nice_ind218('current') }}
