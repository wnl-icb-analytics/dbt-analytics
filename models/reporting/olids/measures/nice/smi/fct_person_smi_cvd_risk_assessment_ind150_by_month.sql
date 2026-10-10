{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND150: https://www.nice.org.uk/indicators/ind150
-- CVD risk assessment in 12 months for people aged 25 to 84 with an active SMI diagnosis, excluding existing CVD, CKD, familial hypercholesterolaemia and type 1 diabetes.
{{ nice_ind150('by_month') }}
