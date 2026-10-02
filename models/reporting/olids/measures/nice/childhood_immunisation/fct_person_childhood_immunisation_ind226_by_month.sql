{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND226: https://www.nice.org.uk/indicators/ind226
-- Two primary MenB doses before 12 months and one booster from 12 months, all before 18 months of age, for children who reached 18 months in the preceding 12 months.
{{ nice_ind226('by_month') }}
