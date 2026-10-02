{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND224: https://www.nice.org.uk/indicators/ind224
-- Two rotavirus doses before 24 weeks of age for babies who reached 24 weeks in the preceding 12 months.
{{ nice_ind224('by_month') }}
