{{ config(materialized='table') }}

-- NICE IND85: https://www.nice.org.uk/indicators/ind85
-- Cervical screening completed in 5 years for women aged 25 to 64 with an active SMI diagnosis.
{{ nice_ind85('current') }}
