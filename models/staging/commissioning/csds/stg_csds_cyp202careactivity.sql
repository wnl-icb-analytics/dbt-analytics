{{ config(materialized='table') }}

-- Existing summaries use this latest (contact, activity) projection.
-- Submitted occurrence detail is retained in stg_csds_care_activity_history.
select
    unique_care_activity_identifier
    , unique_care_contact_identifier
    , community_care_activity_type
    , person_id
    , clinical_contact_duration_of_care_activity
    , coded_procedure_clinical_terminology
    , coded_finding_coded_clinical_entry
    , coded_observation_clinical_terminology
    , effective_from
from {{ ref('stg_csds_care_activity_history') }}
qualify row_number() over (
    partition by unique_care_contact_identifier, unique_care_activity_identifier
    order by effective_from desc
) = 1
