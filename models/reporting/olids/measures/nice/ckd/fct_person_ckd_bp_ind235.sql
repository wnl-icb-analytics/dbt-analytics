{{ config(materialized='table') }}

-- NICE IND235: https://www.nice.org.uk/indicators/ind235
-- Last BP in 12 months below 140/90 clinic or 135/85 home for people on the CKD register with a latest ACR below 70 mg/mmol, without moderate or severe frailty.
{{ nice_ind235('current') }}
