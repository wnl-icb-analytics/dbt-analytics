select
    -- Primary key
    id,

    -- Business columns
    patient_id,
    encounter_id,
    practitioner_id,
    parent_observation_id,
    clinical_effective_date,
    clinical_effective_date_precision_source_concept_id,
    result_value,
    result_measurement_units_source_concept_id,
    result_date,
    result_text,
    is_problem,
    is_review,
    problem_end_date,
    diagnostic_order_source_concept_id,
    age_at_event,
    age_at_event_baby,
    age_at_event_neonate,
    episodicity_source_concept_id,
    is_primary,
    date_recorded,
    person_id,
    publisher_organisation_code,
    lds_transform_datetime,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id

from {{ ref('raw_olids_diagnostic_order') }}
where coalesce(lds_is_deleted, false) = false
    and person_id is not null
