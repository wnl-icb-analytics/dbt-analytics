select
    -- Primary key
    id,

    -- Business columns
    publisher_organisation_id,
    patient_id,
    person_id,
    episode_type_source_concept_id,
    episode_type_code,
    episode_type_display,
    episode_type_source_code,
    episode_type_source_display,
    episode_status_source_concept_id,
    episode_status_code,
    episode_status_display,
    episode_status_source_code,
    episode_status_source_display,
    episode_of_care_start_date,
    episode_of_care_end_date,
    usual_practitioner_in_role_id as care_manager_practitioner_in_role_id,
    managing_organisation_id as care_manager_organisation_id, -- REVIEW: renamed upstream
    managing_organisation_code as care_manager_organisation_code, -- REVIEW: renamed upstream
    publisher_organisation_code,
    lds_transform_datetime,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id,


    -- New columns exposed by the 2026 OLIDS schema realignment (issue #747)
    author_organisation_id
-- Reads Episode of Care V2, which keeps earlier registrations at the same practice.
-- LDS moves EPISODE_OF_CARE onto the V2 logic on 6 October 2026 and removes V2 on
-- 20 October; switch back to raw_olids_episode_of_care between those dates.
from {{ ref('raw_olids_episode_of_care_v2') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
