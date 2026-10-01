{{
    config(materialized = 'table')
}}

select
    -- Primary key
    id,

    -- Business columns
    patient_id,
    person_id,
    person_uuid,
    gp_practice_code,
    lds_business_id_person,

    -- Metadata
    lds_is_deleted,
    lds_source_record_id,
    lds_source_record_id_person,
    lds_transform_datetime
from {{ ref('raw_olids_patient_person') }}
where patient_id is not null
  and person_uuid is not null
  and person_id is not null
qualify row_number() over (
    partition by patient_id, person_uuid
    order by lds_transform_datetime desc, lds_source_record_id desc
) = 1
