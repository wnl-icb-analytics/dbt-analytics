{{ config(materialized='table', cluster_by=['person_id', 'clinical_effective_date']) }}

-- Delivery windows use the clinical date; recording does not move the birth date.
SELECT id, person_id, clinical_effective_date_raw AS clinical_effective_date, date_recorded
FROM ({{ get_observations("'DELIVERY_COD'", source="ECL_CACHE") }})
WHERE clinical_effective_date_raw IS NOT NULL
