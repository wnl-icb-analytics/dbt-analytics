{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND176: https://www.nice.org.uk/indicators/ind176
-- Cervical screening recorded in 3.5 years for women aged 25 to 49; excludes people without a cervix, non-response to three invitations in the screening interval and pregnancy.
{{ nice_ind176('by_month') }}
