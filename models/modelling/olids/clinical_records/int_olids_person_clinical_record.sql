{{ config(materialized='view', tags=['person_clinical_record']) }}

select
    clinical_record_id::varchar as clinical_record_id,
    sk_patient_id::varchar as sk_patient_id,
    person_id::varchar as source_person_id,
    'OLIDS'::varchar as source_dataset,
    source_record_type,
    source_record_id::varchar as source_record_id,
    'fct_gp_clinical_record'::varchar as source_model_name,
    source_record_type as clinical_record_type,
    clinical_record_date::date as clinical_record_date,
    case
        when clinical_record_date is null then 'unknown'
        when clinical_date_precision_code = 'YMD' then 'date'
        when clinical_date_precision_code = 'YM' then 'month'
        when clinical_date_precision_code = 'Y' then 'year'
        else 'unknown'
    end as clinical_time_precision,
    'clinical_effective_date'::varchar as clinical_time_basis,
    source_code,
    source_code_name,
    decode(source_coding_system, 'EMIS_CodeID_cs', 'EMIS', 'snomed_info_sct', 'SNOMED CT', source_coding_system) as source_coding_system,
    iff(s.mapped_coding_system in ('snomed_info_sct', 'SNOMED CT'), s.mapped_code, null) as mapped_code,
    iff(s.mapped_coding_system in ('snomed_info_sct', 'SNOMED CT'), s.mapped_code_name, null) as mapped_code_name,
    decode(mapped_coding_system, 'snomed_info_sct', 'SNOMED CT', mapped_coding_system) as mapped_coding_system,
    result_value::varchar as result_value,
    try_to_decimal(result_value::varchar, 38, 9) as result_value_numeric,
    case
        when result_value is null then 'no value'
        when result_value_numeric is null then 'not numeric or out of range'
        when result_value_numeric::float <> result_value then 'numeric (rounded)'
        else 'numeric'
    end as result_value_parse_status,
    result_date,
    date_recorded::timestamp_ntz as recorded_at,
    result_unit_source_code as result_unit_code,
    result_unit_source_name as result_unit_name,
    medication_name,
    medication_dose,
    medication_quantity_value,
    medication_quantity_unit,
    medication_duration_days,
    medication_authorisation_type_code,
    medication_authorisation_type_name,
    provider_organisation_code,
    provider_organisation_name,
    provider_code_authority,
    is_encounter_person_consistent as is_parent_person_consistent,
    encounter_date::date as parent_start_date,
    case
        when encounter_date is null then 'unknown'
        when encounter_date_precision_code = 'YMD' then 'date'
        when encounter_date_precision_code = 'YM' then 'month'
        when encounter_date_precision_code = 'Y' then 'year'
        else 'unknown'
    end::varchar as parent_start_date_precision,
    iff(is_encounter_person_consistent is distinct from false, encounter_id, null)::varchar as parent_record_id,
    iff(parent_record_id is not null, 'encounter', null)::varchar as parent_record_type,
    iff(parent_record_id is not null, 'stg_olids_encounter', null)::varchar as parent_model_name,
    iff(parent_record_id is not null, 'recorded_parent', null)::varchar as relationship_type,
    source_extraction_date as source_received_at,
    iff(s.mapped_code is not null and s.mapped_coding_system in ('snomed_info_sct', 'SNOMED CT'),
        'OLIDS concept map', null)::varchar as snomed_mapping_method
from {{ ref('fct_gp_clinical_record') }} as s
