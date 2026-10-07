{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND231: https://www.nice.org.uk/indicators/ind231
-- Lipid-lowering therapy in the last 6 months for people on the CKD register without haemorrhagic stroke history.
{{ nice_ind231('by_month') }}
