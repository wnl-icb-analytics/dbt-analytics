select
    -- Primary key
    id,
    source_entity,
    source_record_id,

    -- Business columns
    patient_id,
    person_id,
    encounter_id,
    practitioner_id,
    parent_observation_id,
    clinical_effective_date,
    clinical_effective_date_precision_source_concept_id,
    result_value,
    result_value_units_source_concept_id as result_units_source_concept_id,
    result_date,
    result_text,
    allergy_medication_name,
    is_problem,
    is_review,
    problem_end_date,
    observation_source_concept_id,
    source_code,
    source_display,
    source_system,
    target_system,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    episodicity_source_concept_id,
    is_primary,
    date_recorded,
    is_problem_deleted,
    is_confidential,
    lds_is_deleted,
    publisher_organisation_code,
    lds_transform_datetime,
    mapped_concept_id,
    mapped_concept_code,
    mapped_concept_display,
    result_unit_code,
    result_unit_display,

    -- Metadata
    lds_source_record_id

from {{ ref('raw_olids_observation') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
