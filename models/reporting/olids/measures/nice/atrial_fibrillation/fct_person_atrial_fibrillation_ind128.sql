{{ config(materialized='table') }}

-- NICE IND128: https://www.nice.org.uk/indicators/ind128
-- Oral anticoagulant order in 6 months for people on the AF register with a latest CHA2DS2-VASc of 2 or more, or no CHA2DS2-VASc and a latest CHADS2 of 2 or more; excludes an anticoagulant allergy or adverse reaction ever, or anticoagulation contraindicated in 12 months.
{{ nice_ind128('current') }}
