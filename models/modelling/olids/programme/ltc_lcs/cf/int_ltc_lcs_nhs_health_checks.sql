{{ config(materialized='table') }}
-- CF_NHSHC2Y code list (XML 4d0cb4d9): codes with requested children, using the shared equivalence rule.
-- One row per matching observation, at any date. The case-finding base applies the window.

select
    observation_id as id,
    patient_id,
    person_id,
    clinical_effective_date,
    concept_code,
    concept_display
from ({{ get_ltc_lcs_observations("b4840b00-a8f1-d4e3-4b05-12b4eebfeebc") }})
qualify row_number() over (partition by observation_id order by valueset_id) = 1
