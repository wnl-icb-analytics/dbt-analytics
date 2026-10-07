{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND202: https://www.nice.org.uk/indicators/ind202
-- Brief intervention within 3 months of any qualifying positive screen for people with a listed LTC and a positive screen in the preceding 2 years; excludes alcohol-related disorders.
{{ nice_ind202('by_month') }}
