{{ config(materialized='view') }}

-- NICE IND234: https://www.nice.org.uk/indicators/ind234
-- eGFR and urine ACR both recorded within 90 days before or after diagnosis for people diagnosed with CKD stage 3 to 5 in the preceding 12 months.
{{ nice_ind234('current') }}
