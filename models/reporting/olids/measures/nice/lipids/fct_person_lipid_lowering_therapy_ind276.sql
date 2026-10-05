{{ config(materialized='view') }}

-- NICE IND276: https://www.nice.org.uk/indicators/ind276
-- Lipid-lowering therapy in the last 6 months for people on the diabetes register and the CHD, stroke/TIA or PAD register without haemorrhagic stroke history.
{{ nice_ind276('current') }}
