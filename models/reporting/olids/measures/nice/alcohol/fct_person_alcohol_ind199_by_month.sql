{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND199: https://www.nice.org.uk/indicators/ind199
-- Brief intervention within 3 months of any qualifying positive screen for people aged 10 and over with a first depression or anxiety diagnosis and a positive screen in the preceding 12 months; excludes alcohol-related disorders.
{{ nice_ind199('by_month') }}
