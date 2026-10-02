{{ config(materialized='table', cluster_by=['person_id']) }}

-- NICE IND278: https://www.nice.org.uk/indicators/ind278
-- Latest LDL or non-HDL in 12 months for secondary-prevention CVD, excluding FH and haemorrhagic stroke history.
{{ nice_ind278('current') }}
