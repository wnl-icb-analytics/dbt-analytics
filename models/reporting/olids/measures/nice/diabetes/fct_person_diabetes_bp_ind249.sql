{{ config(materialized='table') }}

-- NICE IND249: https://www.nice.org.uk/indicators/ind249
-- Last BP in 12 months below 140/90 clinic or 135/85 home, diabetes register aged 17 to 79 without moderate or severe frailty.
{{ nice_ind249('current') }}
