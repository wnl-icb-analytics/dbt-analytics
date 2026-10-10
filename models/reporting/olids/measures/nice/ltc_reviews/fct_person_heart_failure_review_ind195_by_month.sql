{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND195: https://www.nice.org.uk/indicators/ind195
-- Heart failure review, NYHA assessment and medication review in 12 months for people on the heart failure register.
{{ nice_ind195('by_month') }}
