{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND177: https://www.nice.org.uk/indicators/ind177
-- Cervical screening recorded in 5.5 years for women aged 50 to 64; excludes people without a cervix, non-response to three invitations in the screening interval and pregnancy.
{{ nice_ind177('by_month') }}
