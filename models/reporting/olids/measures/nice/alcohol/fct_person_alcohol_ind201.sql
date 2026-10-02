{{ config(materialized='view') }}

-- NICE IND201: https://www.nice.org.uk/indicators/ind201
-- FAST or AUDIT-C screen in the preceding 2 years for people with a listed LTC; excludes alcohol-related disorders.
{{ nice_ind201('current') }}
