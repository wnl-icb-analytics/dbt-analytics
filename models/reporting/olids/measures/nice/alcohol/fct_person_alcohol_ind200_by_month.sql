{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND200: https://www.nice.org.uk/indicators/ind200
-- Brief intervention after a positive screen in 12 months for active diagnosed SMI; lithium therapy alone does not qualify.
{{ nice_ind200('by_month') }}
