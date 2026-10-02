{{ config(materialized='view') }}

-- NICE IND104: https://www.nice.org.uk/indicators/ind104
-- Depression review 10 to 35 days after a new diagnosis for adults diagnosed since 1 April, using QOF's interim annual reporting period.
{{ nice_ind104('current') }}
