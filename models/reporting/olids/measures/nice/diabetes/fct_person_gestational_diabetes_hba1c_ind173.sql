{{ config(materialized='table') }}

-- NICE IND173: https://www.nice.org.uk/indicators/ind173
-- HbA1c in 12 months for women whose latest gestational diabetes episode is more than 12 months old, excluding diabetes diagnosed more than 12 months ago (NICE pilot report reading).
{{ nice_ind173('current') }}
