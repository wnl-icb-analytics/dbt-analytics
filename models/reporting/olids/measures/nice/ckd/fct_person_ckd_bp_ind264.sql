{{ config(materialized='view') }}

-- NICE IND264: https://www.nice.org.uk/indicators/ind264
-- Last BP in 12 months below 130/80 clinic or 125/75 home for people on the CKD register with a latest ACR of 70 mg/mmol or more, without moderate or severe frailty.
{{ nice_ind264('current') }}
