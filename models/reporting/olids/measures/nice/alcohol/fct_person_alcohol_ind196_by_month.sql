{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND196: https://www.nice.org.uk/indicators/ind196
-- FAST or AUDIT-C screen within 3 months either side of a first hypertension diagnosis in the preceding 12 months; excludes alcohol-related disorders.
{{ nice_ind196('by_month') }}
