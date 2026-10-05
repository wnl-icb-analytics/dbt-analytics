{{ config(materialized='view') }}

-- NICE IND324: https://www.nice.org.uk/indicators/ind324
-- SGLT2 inhibitor order in 6 months, preceded by renin-angiotensin treatment outside type 2 diabetes, for people on the CKD register with type 2 diabetes, or without it and on (or contraindicated to) ACE inhibitor or ARB therapy with eGFR 20 to 44, or eGFR 45 to 59 with ACR 22.6 or more; excludes eGFR below 20.
{{ nice_ind324('current') }}
