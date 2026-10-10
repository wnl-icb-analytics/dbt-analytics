{{ config(materialized='table') }}

-- NICE IND230: https://www.nice.org.uk/indicators/ind230
-- Lipid-lowering therapy in the last 6 months for people on the CHD, stroke/TIA or PAD register without haemorrhagic stroke history.
{{ nice_ind230('current') }}
