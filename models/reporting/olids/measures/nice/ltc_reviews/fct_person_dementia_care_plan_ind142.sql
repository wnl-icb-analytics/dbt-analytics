{{ config(materialized='view') }}

-- NICE IND142: https://www.nice.org.uk/indicators/ind142
-- Dementia care plan or review in 12 months, on or after diagnosis; the face-to-face setting is not coded.
{{ nice_ind142('current') }}
