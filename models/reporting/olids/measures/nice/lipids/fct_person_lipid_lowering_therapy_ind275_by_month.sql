{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND275: https://www.nice.org.uk/indicators/ind275
-- Lipid-lowering therapy in 6 months for people with diabetes aged 40 and over, no CVD, no moderate or severe frailty; excludes type 2 diabetes with a recent risk score below 10% unless a later score reaches 10%.
{{ nice_ind275('by_month') }}
