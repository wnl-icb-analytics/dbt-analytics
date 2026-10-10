{{ config(materialized='table') }}

-- NICE IND266: https://www.nice.org.uk/indicators/ind266
-- Learning disability health check and health action plan in 12 months plus a recorded ethnicity for people on the learning disability register.
{{ nice_ind266('current') }}
