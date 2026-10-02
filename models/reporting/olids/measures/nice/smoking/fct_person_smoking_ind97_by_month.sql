{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND97: https://www.nice.org.uk/indicators/ind97
-- Smoking status recorded in 12 months for people with a listed LTC including schizophrenia, bipolar disorder and other psychoses; a never-smoker reaching 26 by the end of the financial year is covered by a never-smoked record made after their 25th birthday and after their earliest listed diagnosis.
{{ nice_ind97('by_month') }}
