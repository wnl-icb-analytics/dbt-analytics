select
    code_set_name,
    source_code_set_name,
    code,
    description,
    short_description,
    category,
    notes,
    valid_from_date,
    valid_to_date,
    is_currently_valid,
    source_unique_key,
    source_imported_at,
    source_created_at,
    is_latest_definition,
    source_effective_from_at,
    source_effective_to_at
from {{ ref('stg_ukhfd_data_dictionary_mhsds_inpatient_code_lookup') }}

union all

select
    'mental_health_admitted_patient_classification' as code_set_name,
    source_code_set_name,
    code,
    description,
    short_description,
    category,
    notes,
    valid_from_date,
    valid_to_date,
    is_currently_valid,
    source_unique_key,
    source_imported_at,
    source_created_at,
    is_latest_definition,
    source_effective_from_at,
    source_effective_to_at
from {{ ref('mental_health_admitted_patient_classification_type_history') }}
