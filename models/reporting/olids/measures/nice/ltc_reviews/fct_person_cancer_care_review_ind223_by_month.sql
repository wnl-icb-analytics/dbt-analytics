{{ config(materialized='table', cluster_by=['reporting_date', 'person_id'], tags=['monthly-full', 'nice-history']) }}

-- NICE IND223: https://www.nice.org.uk/indicators/ind223
-- Cancer care review within 12 months of the latest new cancer diagnosis for people diagnosed in the preceding 24 months; the register excludes non-melanoma skin cancer.
{{ nice_ind223('by_month') }}
