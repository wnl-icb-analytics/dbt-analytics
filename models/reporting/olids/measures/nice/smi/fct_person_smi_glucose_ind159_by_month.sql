{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND159: https://www.nice.org.uk/indicators/ind159
-- Blood glucose or HbA1c recorded in 12 months for adults with an active SMI diagnosis, excluding diabetes diagnosed more than 12 months ago.
{{ nice_ind159('by_month') }}
