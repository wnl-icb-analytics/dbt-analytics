select
    -- Primary key
    lds_source_record_id,

    -- Business columns
    id,
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
    managing_organisation_id as care_manager_organisation_id,
    managing_organisation_code as care_manager_organisation_code,
    author_organisation_id,
    publisher_organisation_code,
    type,
    status,
    clinical_system,

    -- Metadata
    source_extraction_date,
    lds_transform_datetime,
    lds_is_deleted

from {{ ref('raw_olids_episode_of_care_v2') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
