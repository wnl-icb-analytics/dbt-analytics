-- int_sus_uec_encounter selects the primary diagnosis with is_primary, while
-- int_sus_uec_encounter_clinical_codes excludes source position 1 to leave it
-- out of the secondary arrays. Between them the two models cover the diagnosis
-- feed only while those rules pick the same row. A diagnosis flagged primary
-- away from position 1 would be duplicated across both models, and the
-- position-1 row it displaced would appear in neither.
--
-- is_primary is null rather than false on a secondary row, and both sides
-- coalesce so a null never silently satisfies the comparison.
select
    primarykey_id
    , snomed_id
    , is_primary
from {{ ref('stg_sus_ecds_clinical_diagnoses_snomed') }}
where code is not null
  and coalesce(is_primary, false) <> coalesce(snomed_id = 1, false)
