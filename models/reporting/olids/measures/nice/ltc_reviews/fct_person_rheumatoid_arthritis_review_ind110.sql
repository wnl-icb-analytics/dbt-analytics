{{ config(materialized='view') }}

-- NICE IND110: https://www.nice.org.uk/indicators/ind110
-- Rheumatoid arthritis annual review recorded in 15 months for people on the rheumatoid arthritis register.
{{ nice_ind110('current') }}
