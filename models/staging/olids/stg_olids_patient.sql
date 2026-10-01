select
    -- Primary key
    id,

    -- Business columns
    {{ consistent_sk_patient_id_format('sk_patient_id')}} as sk_patient_id,
    title,
    gender_source_concept_id,
    gender_code,
    gender_display,
    gender_source_code,
    gender_source_display,
    registered_practice_organisation_id,
    birth_year,
    birth_month,
    death_year,
    death_month,
    is_spine_sensitive,
    is_confidential,
    is_test_patient,
    publisher_organisation_code,
    lds_transform_datetime,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id,


    -- New columns exposed by the 2026 OLIDS schema realignment (issue #747)
    person_id,
    local_patient_id
from {{ ref('raw_olids_patient') }}
where coalesce(lds_is_deleted, false) = false
