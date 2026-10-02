{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

-- NICE IND154: https://www.nice.org.uk/indicators/ind154
-- Smoking status recorded in 12 months for people with an active SMI diagnosis; a never-smoker reaching 26 by the end of the financial year is covered by a never-smoked record made after their 25th birthday and after their earliest SMI diagnosis.
{{ nice_ind154('by_month') }}
