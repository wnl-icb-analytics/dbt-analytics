{{ config(
    materialized='incremental', incremental_strategy='delete+insert',
    unique_key=['source_record_type', 'source_record_id'], on_schema_change='fail',
    cluster_by=['sk_patient_id', 'clinical_record_date'], tags=['person_clinical_record', 'daily'],
    pre_hook="{{ navigation_build_warehouse() }}",
    post_hook=["{{ navigation_remove_withdrawn_records([('apc_diagnosis', 'stg_sus_apc_spell_episodes_clinical_coding_diagnosis_icd', 'diagnosis_id'), ('apc_procedure', 'stg_sus_apc_spell_episodes_clinical_coding_procedure_opcs', 'procedure_id'), ('op_diagnosis', 'stg_sus_op_appointment_clinical_coding_diagnosis_icd', 'diagnosis_id'), ('op_procedure', 'stg_sus_op_appointment_clinical_coding_procedure_opcs', 'procedure_id')]) }}", "{{ navigation_build_warehouse(restore=true) }}"]
) }}

with clinical_records as (
select
    p.sk_patient_id::varchar as sk_patient_id,
    'SUS_APC'::varchar as source_dataset,
    'apc_diagnosis'::varchar as source_record_type,
    s.diagnosis_id::varchar as source_record_id,
    'stg_sus_apc_spell_episodes_clinical_coding_diagnosis_icd'::varchar as source_model_name,
    'diagnosis'::varchar as clinical_record_type,
    null::date as clinical_record_date,
    'unknown'::varchar as clinical_time_precision,
    'not_recorded'::varchar as clinical_time_basis,
    s.code::varchar as source_code,
    label.description::varchar as source_code_name,
    'ICD-10'::varchar as source_coding_system,
    p.organisation_id::varchar as provider_organisation_code,
    p.organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    {{ dbt_utils.generate_surrogate_key(['s.primarykey_id', 's.episodes_id']) }}::varchar as parent_record_id,
    'hospital_episode'::varchar as parent_record_type,
    'stg_sus_apc_spell_episodes'::varchar as parent_model_name,
    'recorded_parent'::varchar as relationship_type,
    s.icd_id::number as coding_position,
    s.present_on_admission::varchar as present_on_admission_code,
    p.sk_patient_id::varchar = episode.sk_patient_id::varchar as is_parent_person_consistent,
    iff(is_parent_person_consistent, episode.start_date, null)::date as parent_start_date,
    iff(is_parent_person_consistent, episode.end_date, null)::date as parent_end_date,
    delivery.source_received_at::timestamp_ntz as source_received_at
from {{ ref('stg_sus_apc_spell_episodes_clinical_coding_diagnosis_icd') }} as s
left join {{ ref('obt_encounter_apc') }} as p on s.primarykey_id = p.visit_occurrence_id
left join {{ ref('stg_sus_apc_spell_episodes') }} as episode
    on s.primarykey_id = episode.primarykey_id and s.episodes_id = episode.episodes_id
left join {{ ref('stg_sus_apc_spell') }} as delivery on s.primarykey_id = delivery.primarykey_id
left join {{ ref('icd10_code') }} as label on replace(upper(trim(s.code)), '.', '') = label.code
where true
{{ navigation_delivery_filter('delivery.source_received_at', 'apc_diagnosis') }}
union all
select
    p.sk_patient_id::varchar as sk_patient_id,
    'SUS_APC'::varchar as source_dataset,
    'apc_procedure'::varchar as source_record_type,
    s.procedure_id::varchar as source_record_id,
    'stg_sus_apc_spell_episodes_clinical_coding_procedure_opcs'::varchar as source_model_name,
    'procedure'::varchar as clinical_record_type,
    s.procedure_date::date as clinical_record_date,
    case when s.procedure_date is null then 'unknown' else 'date' end::varchar as clinical_time_precision,
    'procedure_date'::varchar as clinical_time_basis,
    s.code::varchar as source_code,
    label.description::varchar as source_code_name,
    'OPCS-4'::varchar as source_coding_system,
    p.organisation_id::varchar as provider_organisation_code,
    p.organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    {{ dbt_utils.generate_surrogate_key(['s.primarykey_id', 's.episodes_id']) }}::varchar as parent_record_id,
    'hospital_episode'::varchar as parent_record_type,
    'stg_sus_apc_spell_episodes'::varchar as parent_model_name,
    'recorded_parent'::varchar as relationship_type,
    s.opcs_id::number as coding_position,
    null::varchar as present_on_admission_code,
    p.sk_patient_id::varchar = episode.sk_patient_id::varchar as is_parent_person_consistent,
    iff(is_parent_person_consistent, episode.start_date, null)::date as parent_start_date,
    iff(is_parent_person_consistent, episode.end_date, null)::date as parent_end_date,
    delivery.source_received_at::timestamp_ntz as source_received_at
from {{ ref('stg_sus_apc_spell_episodes_clinical_coding_procedure_opcs') }} as s
left join {{ ref('obt_encounter_apc') }} as p on s.primarykey_id = p.visit_occurrence_id
left join {{ ref('stg_sus_apc_spell_episodes') }} as episode
    on s.primarykey_id = episode.primarykey_id and s.episodes_id = episode.episodes_id
left join {{ ref('stg_sus_apc_spell') }} as delivery on s.primarykey_id = delivery.primarykey_id
left join {{ ref('opcs4_code') }} as label on replace(upper(trim(s.code)), '.', '') = label.code
where true
{{ navigation_delivery_filter('delivery.source_received_at', 'apc_procedure') }}
union all
select
    p.sk_patient_id::varchar as sk_patient_id,
    'SUS_OP'::varchar as source_dataset,
    'op_diagnosis'::varchar as source_record_type,
    s.diagnosis_id::varchar as source_record_id,
    'stg_sus_op_appointment_clinical_coding_diagnosis_icd'::varchar as source_model_name,
    'diagnosis'::varchar as clinical_record_type,
    null::date as clinical_record_date,
    'unknown'::varchar as clinical_time_precision,
    'not_recorded'::varchar as clinical_time_basis,
    s.code::varchar as source_code,
    label.description::varchar as source_code_name,
    'ICD-10'::varchar as source_coding_system,
    p.organisation_id::varchar as provider_organisation_code,
    p.organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    s.primarykey_id::varchar as parent_record_id,
    'outpatient_appointment'::varchar as parent_record_type,
    'int_sus_op_appointment'::varchar as parent_model_name,
    'recorded_parent'::varchar as relationship_type,
    s.icd_id::number as coding_position,
    s.present_on_admission::varchar as present_on_admission_code,
    null::boolean as is_parent_person_consistent,
    p.start_date::date as parent_start_date,
    null::date as parent_end_date,
    delivery.source_received_at::timestamp_ntz as source_received_at
from {{ ref('stg_sus_op_appointment_clinical_coding_diagnosis_icd') }} as s
left join {{ ref('int_sus_op_appointment') }} as p on s.primarykey_id = p.visit_occurrence_id
left join {{ ref('stg_sus_op_appointment') }} as delivery on s.primarykey_id = delivery.primarykey_id
left join {{ ref('icd10_code') }} as label on replace(upper(trim(s.code)), '.', '') = label.code
where true
{{ navigation_delivery_filter('delivery.source_received_at', 'op_diagnosis') }}
union all
select
    p.sk_patient_id::varchar as sk_patient_id,
    'SUS_OP'::varchar as source_dataset,
    'op_procedure'::varchar as source_record_type,
    s.procedure_id::varchar as source_record_id,
    'stg_sus_op_appointment_clinical_coding_procedure_opcs'::varchar as source_model_name,
    'procedure'::varchar as clinical_record_type,
    s.procedure_date::date as clinical_record_date,
    case when s.procedure_date is null then 'unknown' else 'date' end::varchar as clinical_time_precision,
    'procedure_date'::varchar as clinical_time_basis,
    s.code::varchar as source_code,
    label.description::varchar as source_code_name,
    'OPCS-4'::varchar as source_coding_system,
    p.organisation_id::varchar as provider_organisation_code,
    p.organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    s.primarykey_id::varchar as parent_record_id,
    'outpatient_appointment'::varchar as parent_record_type,
    'int_sus_op_appointment'::varchar as parent_model_name,
    'recorded_parent'::varchar as relationship_type,
    s.opcs_id::number as coding_position,
    null::varchar as present_on_admission_code,
    null::boolean as is_parent_person_consistent,
    p.start_date::date as parent_start_date,
    null::date as parent_end_date,
    delivery.source_received_at::timestamp_ntz as source_received_at
from {{ ref('stg_sus_op_appointment_clinical_coding_procedure_opcs') }} as s
left join {{ ref('int_sus_op_appointment') }} as p on s.primarykey_id = p.visit_occurrence_id
left join {{ ref('stg_sus_op_appointment') }} as delivery on s.primarykey_id = delivery.primarykey_id
left join {{ ref('opcs4_code') }} as label on replace(upper(trim(s.code)), '.', '') = label.code
where true
{{ navigation_delivery_filter('delivery.source_received_at', 'op_procedure') }}
)
select
    {{ dbt_utils.generate_surrogate_key(['source_dataset', 'source_record_type', 'source_record_id']) }} as clinical_record_id,
    sk_patient_id,
    source_dataset,
    source_record_type,
    source_record_id,
    source_model_name,
    clinical_record_type,
    clinical_record_date,
    clinical_time_precision,
    clinical_time_basis,
    source_code,
    source_code_name,
    source_coding_system,
    provider_organisation_code,
    provider_organisation_name,
    provider_code_authority,
    iff(is_parent_person_consistent is distinct from false, parent_record_id, null) as parent_record_id,
    iff(is_parent_person_consistent is distinct from false, parent_record_type, null) as parent_record_type,
    iff(is_parent_person_consistent is distinct from false, parent_model_name, null) as parent_model_name,
    iff(is_parent_person_consistent is distinct from false, relationship_type, null) as relationship_type,
    coding_position,
    present_on_admission_code,
    is_parent_person_consistent,
    parent_start_date,
    parent_end_date,
    source_received_at
from clinical_records
