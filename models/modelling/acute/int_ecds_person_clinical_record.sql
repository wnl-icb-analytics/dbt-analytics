{{ config(
    materialized='incremental', incremental_strategy='delete+insert',
    unique_key=['source_record_type', 'source_record_id'], on_schema_change='fail',
    cluster_by=['sk_patient_id', 'coalesce(clinical_record_at, clinical_record_date::timestamp_ntz)'], tags=['person_clinical_record', 'daily'],
    pre_hook="{{ navigation_build_warehouse() }}",
    post_hook=["{{ navigation_remove_withdrawn_records([('diagnosis', 'stg_sus_ecds_clinical_diagnoses_snomed', 'diagnosis_id'), ('treatment', 'stg_sus_ecds_clinical_treatments_snomed', 'source_record_id'), ('investigation', 'stg_sus_ecds_clinical_investigations_snomed', 'source_record_id'), ('comorbidity', 'stg_sus_ecds_clinical_comorbidities', 'source_record_id'), ('clinical_finding', 'stg_sus_ecds_clinical_coded_findings', 'source_record_id'), ('observation', 'fct_sus_uec_observation', 'observation_id'), ('scored_assessment', 'fct_sus_uec_scored_assessment', 'assessment_id'), ('chief_complaint', 'obt_encounter_uec', 'iff(chief_complaint_code is not null, visit_occurrence_id, null)'), ('acuity', 'obt_encounter_uec', 'iff(acuity is not null, visit_occurrence_id, null)'), ('notifiable_disease', 'obt_encounter_uec', 'iff(disease_notification_code is not null, visit_occurrence_id, null)'), ('injury_mechanism', 'obt_encounter_uec', 'iff(injury_mechanism_code is not null, visit_occurrence_id, null)'), ('injury_intent', 'obt_encounter_uec', 'iff(injury_intent_code is not null, visit_occurrence_id, null)'), ('injury_place', 'obt_encounter_uec', 'iff(place_of_injury_code is not null, visit_occurrence_id, null)'), ('injury_alcohol_drug_involvement', 'int_sus_uec_injury_alcohol_drug', 'involvement_id')]) }}", "{{ navigation_build_warehouse(restore=true) }}"]
) }}

with attendance as (
select
    p.visit_occurrence_id,
    p.sk_patient_id,
    p.organisation_id,
    p.organisation_name,
    p.start_date,
    p.end_date,
    p.chief_complaint_code,
    p.chief_complaint_desc,
    p.acuity,
    p.acuity_desc,
    p.disease_notification_code,
    p.disease_notification_desc,
    p.injury_mechanism_code,
    p.injury_mechanism_desc,
    p.injury_intent_code,
    p.injury_intent_desc,
    p.place_of_injury_code,
    p.place_of_injury_desc,
    p.injury_date,
    p.injury_time,
    delivery.source_received_at
from {{ ref('obt_encounter_uec') }} as p
left join {{ ref('stg_sus_ecds_emergency_care') }} as delivery on p.visit_occurrence_id = delivery.primarykey_id
),

-- Single-valued attendance fields become one row per recorded code. Injury details take the recorded injury date.
attendance_items as (
select visit_occurrence_id, 'chief_complaint'::varchar as item_type, visit_occurrence_id::varchar as source_record_id,
    'obt_encounter_uec'::varchar as source_model_name, chief_complaint_code::varchar as code,
    chief_complaint_desc::varchar as code_name, null::date as item_date, null::time as item_time
from attendance where chief_complaint_code is not null
{{ navigation_delivery_filter('source_received_at', 'chief_complaint') }}
union all
select visit_occurrence_id, 'acuity', visit_occurrence_id::varchar, 'obt_encounter_uec', acuity, acuity_desc, null::date, null::time
from attendance where acuity is not null
{{ navigation_delivery_filter('source_received_at', 'acuity') }}
union all
select visit_occurrence_id, 'notifiable_disease', visit_occurrence_id::varchar, 'obt_encounter_uec', disease_notification_code, disease_notification_desc, null::date, null::time
from attendance where disease_notification_code is not null
{{ navigation_delivery_filter('source_received_at', 'notifiable_disease') }}
union all
select visit_occurrence_id, 'injury_mechanism', visit_occurrence_id::varchar, 'obt_encounter_uec', injury_mechanism_code, injury_mechanism_desc, injury_date, injury_time
from attendance where injury_mechanism_code is not null
{{ navigation_delivery_filter('source_received_at', 'injury_mechanism') }}
union all
select visit_occurrence_id, 'injury_intent', visit_occurrence_id::varchar, 'obt_encounter_uec', injury_intent_code, injury_intent_desc, injury_date, injury_time
from attendance where injury_intent_code is not null
{{ navigation_delivery_filter('source_received_at', 'injury_intent') }}
union all
select visit_occurrence_id, 'injury_place', visit_occurrence_id::varchar, 'obt_encounter_uec', place_of_injury_code, place_of_injury_desc, injury_date, injury_time
from attendance where place_of_injury_code is not null
{{ navigation_delivery_filter('source_received_at', 'injury_place') }}
union all
select a.visit_occurrence_id, 'injury_alcohol_drug_involvement', i.involvement_id, 'int_sus_uec_injury_alcohol_drug', i.involvement_code, i.involvement_desc, a.injury_date, a.injury_time
from {{ ref('int_sus_uec_injury_alcohol_drug') }} as i
inner join attendance as a on i.visit_occurrence_id = a.visit_occurrence_id
where i.involvement_code is not null
{{ navigation_delivery_filter('a.source_received_at', 'injury_alcohol_drug_involvement') }}
),

clinical_records as (
select
    p.sk_patient_id::varchar as sk_patient_id,
    'ECDS'::varchar as source_dataset,
    'diagnosis'::varchar as source_record_type,
    s.diagnosis_id::varchar as source_record_id,
    'stg_sus_ecds_clinical_diagnoses_snomed'::varchar as source_model_name,
    'diagnosis'::varchar as clinical_record_type,
    null::date as clinical_record_date,
    'unknown'::varchar as clinical_time_precision,
    'not_recorded'::varchar as clinical_time_basis,
    s.code::varchar as source_code,
    label.preferred_term::varchar as source_code_name,
    'SNOMED CT'::varchar as source_coding_system,
    p.organisation_id::varchar as provider_organisation_code,
    p.organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    s.primarykey_id::varchar as parent_record_id,
    'emergency_care_attendance'::varchar as parent_record_type,
    'obt_encounter_uec'::varchar as parent_model_name,
    'recorded_parent'::varchar as relationship_type,
    s.is_primary::boolean as is_primary_diagnosis,
    s.snomed_id::number as coding_position,
    p.start_date::date as parent_start_date,
    p.end_date::date as parent_end_date,
    delivery.source_received_at::timestamp_ntz as source_received_at,
    null::timestamp_ntz as clinical_record_at,
    s.qualifier::varchar as qualifier_code,
    qualifier.preferred_term::varchar as qualifier_name,
    null::varchar as result_value,
    null::varchar as result_value_name,
    null::number(38,9) as result_value_numeric,
    null::varchar as result_value_parse_status,
    null::varchar as result_unit_code,
    null::varchar as result_unit_name,
    null::varchar as result_unit_symbol,
    null::varchar as assessment_tool_name,
    null::varchar as assessment_response_status
from {{ ref('stg_sus_ecds_clinical_diagnoses_snomed') }} as s
left join {{ ref('obt_encounter_uec') }} as p on s.primarykey_id = p.visit_occurrence_id
left join {{ ref('stg_sus_ecds_emergency_care') }} as delivery on s.primarykey_id = delivery.primarykey_id
left join {{ ref('stg_dictionary_snomed_concept') }} as label on s.code::varchar = label.snomed_code::varchar
left join {{ ref('stg_dictionary_snomed_concept') }} as qualifier on s.qualifier::varchar = qualifier.snomed_code::varchar
where true
{{ navigation_delivery_filter('delivery.source_received_at', 'diagnosis') }}
union all
select
    p.sk_patient_id::varchar as sk_patient_id,
    'ECDS'::varchar as source_dataset,
    'treatment'::varchar as source_record_type,
    s.source_record_id::varchar as source_record_id,
    'stg_sus_ecds_clinical_treatments_snomed'::varchar as source_model_name,
    'treatment'::varchar as clinical_record_type,
    s.date::date as clinical_record_date,
    case when s.date is null then 'unknown' when s.time is not null then 'timestamp' else 'date' end::varchar as clinical_time_precision,
    'date'::varchar as clinical_time_basis,
    s.code::varchar as source_code,
    label.preferred_term::varchar as source_code_name,
    'SNOMED CT'::varchar as source_coding_system,
    p.organisation_id::varchar as provider_organisation_code,
    p.organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    s.primarykey_id::varchar as parent_record_id,
    'emergency_care_attendance'::varchar as parent_record_type,
    'obt_encounter_uec'::varchar as parent_model_name,
    'recorded_parent'::varchar as relationship_type,
    null::boolean as is_primary_diagnosis,
    null::number as coding_position,
    p.start_date::date as parent_start_date,
    p.end_date::date as parent_end_date,
    delivery.source_received_at::timestamp_ntz as source_received_at,
    timestamp_ntz_from_parts(s.date::date, s.time::time)::timestamp_ntz as clinical_record_at,
    null::varchar as qualifier_code,
    null::varchar as qualifier_name,
    null::varchar as result_value,
    null::varchar as result_value_name,
    null::number(38,9) as result_value_numeric,
    null::varchar as result_value_parse_status,
    null::varchar as result_unit_code,
    null::varchar as result_unit_name,
    null::varchar as result_unit_symbol,
    null::varchar as assessment_tool_name,
    null::varchar as assessment_response_status
from {{ ref('stg_sus_ecds_clinical_treatments_snomed') }} as s
left join {{ ref('obt_encounter_uec') }} as p on s.primarykey_id = p.visit_occurrence_id
left join {{ ref('stg_sus_ecds_emergency_care') }} as delivery on s.primarykey_id = delivery.primarykey_id
left join {{ ref('stg_dictionary_snomed_concept') }} as label on s.code::varchar = label.snomed_code::varchar
where true
{{ navigation_delivery_filter('delivery.source_received_at', 'treatment') }}
union all
select
    p.sk_patient_id::varchar as sk_patient_id,
    'ECDS'::varchar as source_dataset,
    'investigation'::varchar as source_record_type,
    s.source_record_id::varchar as source_record_id,
    'stg_sus_ecds_clinical_investigations_snomed'::varchar as source_model_name,
    'investigation'::varchar as clinical_record_type,
    s.date::date as clinical_record_date,
    case when s.date is null then 'unknown' when s.time is not null then 'timestamp' else 'date' end::varchar as clinical_time_precision,
    'date'::varchar as clinical_time_basis,
    s.code::varchar as source_code,
    label.preferred_term::varchar as source_code_name,
    'SNOMED CT'::varchar as source_coding_system,
    p.organisation_id::varchar as provider_organisation_code,
    p.organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    s.primarykey_id::varchar as parent_record_id,
    'emergency_care_attendance'::varchar as parent_record_type,
    'obt_encounter_uec'::varchar as parent_model_name,
    'recorded_parent'::varchar as relationship_type,
    null::boolean as is_primary_diagnosis,
    null::number as coding_position,
    p.start_date::date as parent_start_date,
    p.end_date::date as parent_end_date,
    delivery.source_received_at::timestamp_ntz as source_received_at,
    timestamp_ntz_from_parts(s.date::date, s.time::time)::timestamp_ntz as clinical_record_at,
    null::varchar as qualifier_code,
    null::varchar as qualifier_name,
    null::varchar as result_value,
    null::varchar as result_value_name,
    null::number(38,9) as result_value_numeric,
    null::varchar as result_value_parse_status,
    null::varchar as result_unit_code,
    null::varchar as result_unit_name,
    null::varchar as result_unit_symbol,
    null::varchar as assessment_tool_name,
    null::varchar as assessment_response_status
from {{ ref('stg_sus_ecds_clinical_investigations_snomed') }} as s
left join {{ ref('obt_encounter_uec') }} as p on s.primarykey_id = p.visit_occurrence_id
left join {{ ref('stg_sus_ecds_emergency_care') }} as delivery on s.primarykey_id = delivery.primarykey_id
left join {{ ref('stg_dictionary_snomed_concept') }} as label on s.code::varchar = label.snomed_code::varchar
where true
{{ navigation_delivery_filter('delivery.source_received_at', 'investigation') }}
union all
select
    p.sk_patient_id::varchar as sk_patient_id,
    'ECDS'::varchar as source_dataset,
    'comorbidity'::varchar as source_record_type,
    s.source_record_id::varchar as source_record_id,
    'stg_sus_ecds_clinical_comorbidities'::varchar as source_model_name,
    'comorbidity'::varchar as clinical_record_type,
    null::date as clinical_record_date,
    'unknown'::varchar as clinical_time_precision,
    'not_recorded'::varchar as clinical_time_basis,
    s.code::varchar as source_code,
    label.preferred_term::varchar as source_code_name,
    'SNOMED CT'::varchar as source_coding_system,
    p.organisation_id::varchar as provider_organisation_code,
    p.organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    s.primarykey_id::varchar as parent_record_id,
    'emergency_care_attendance'::varchar as parent_record_type,
    'obt_encounter_uec'::varchar as parent_model_name,
    'recorded_parent'::varchar as relationship_type,
    null::boolean as is_primary_diagnosis,
    null::number as coding_position,
    p.start_date::date as parent_start_date,
    p.end_date::date as parent_end_date,
    delivery.source_received_at::timestamp_ntz as source_received_at,
    null::timestamp_ntz as clinical_record_at,
    null::varchar as qualifier_code,
    null::varchar as qualifier_name,
    null::varchar as result_value,
    null::varchar as result_value_name,
    null::number(38,9) as result_value_numeric,
    null::varchar as result_value_parse_status,
    null::varchar as result_unit_code,
    null::varchar as result_unit_name,
    null::varchar as result_unit_symbol,
    null::varchar as assessment_tool_name,
    null::varchar as assessment_response_status
from {{ ref('stg_sus_ecds_clinical_comorbidities') }} as s
left join {{ ref('obt_encounter_uec') }} as p on s.primarykey_id = p.visit_occurrence_id
left join {{ ref('stg_sus_ecds_emergency_care') }} as delivery on s.primarykey_id = delivery.primarykey_id
left join {{ ref('stg_dictionary_snomed_concept') }} as label on s.code::varchar = label.snomed_code::varchar
where true
{{ navigation_delivery_filter('delivery.source_received_at', 'comorbidity') }}
union all
select
    p.sk_patient_id::varchar as sk_patient_id,
    'ECDS'::varchar as source_dataset,
    'clinical_finding'::varchar as source_record_type,
    s.source_record_id::varchar as source_record_id,
    'stg_sus_ecds_clinical_coded_findings'::varchar as source_model_name,
    'clinical_finding'::varchar as clinical_record_type,
    null::date as clinical_record_date,
    'unknown'::varchar as clinical_time_precision,
    'not_recorded'::varchar as clinical_time_basis,
    s.code::varchar as source_code,
    label.preferred_term::varchar as source_code_name,
    'SNOMED CT'::varchar as source_coding_system,
    p.organisation_id::varchar as provider_organisation_code,
    p.organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    s.primarykey_id::varchar as parent_record_id,
    'emergency_care_attendance'::varchar as parent_record_type,
    'obt_encounter_uec'::varchar as parent_model_name,
    'recorded_parent'::varchar as relationship_type,
    null::boolean as is_primary_diagnosis,
    null::number as coding_position,
    p.start_date::date as parent_start_date,
    p.end_date::date as parent_end_date,
    delivery.source_received_at::timestamp_ntz as source_received_at,
    null::timestamp_ntz as clinical_record_at,
    null::varchar as qualifier_code,
    null::varchar as qualifier_name,
    null::varchar as result_value,
    null::varchar as result_value_name,
    null::number(38,9) as result_value_numeric,
    null::varchar as result_value_parse_status,
    null::varchar as result_unit_code,
    null::varchar as result_unit_name,
    null::varchar as result_unit_symbol,
    null::varchar as assessment_tool_name,
    null::varchar as assessment_response_status
from {{ ref('stg_sus_ecds_clinical_coded_findings') }} as s
left join {{ ref('obt_encounter_uec') }} as p on s.primarykey_id = p.visit_occurrence_id
left join {{ ref('stg_sus_ecds_emergency_care') }} as delivery on s.primarykey_id = delivery.primarykey_id
left join {{ ref('stg_dictionary_snomed_concept') }} as label on s.code::varchar = label.snomed_code::varchar
where true
{{ navigation_delivery_filter('delivery.source_received_at', 'clinical_finding') }}
union all
select
    s.sk_patient_id::varchar as sk_patient_id,
    'ECDS'::varchar as source_dataset,
    'observation'::varchar as source_record_type,
    s.observation_id::varchar as source_record_id,
    'fct_sus_uec_observation'::varchar as source_model_name,
    'observation'::varchar as clinical_record_type,
    s.observed_at::date as clinical_record_date,
    iff(s.observed_at is null, 'unknown', 'timestamp')::varchar as clinical_time_precision,
    iff(s.observed_at is null, 'not_recorded', 'observed_at')::varchar as clinical_time_basis,
    s.observation_code::varchar as source_code,
    s.observation_description::varchar as source_code_name,
    'SNOMED CT'::varchar as source_coding_system,
    s.organisation_id::varchar as provider_organisation_code,
    s.organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    s.visit_occurrence_id::varchar as parent_record_id,
    'emergency_care_attendance'::varchar as parent_record_type,
    'obt_encounter_uec'::varchar as parent_model_name,
    'recorded_parent'::varchar as relationship_type,
    null::boolean as is_primary_diagnosis,
    null::number as coding_position,
    p.start_date::date as parent_start_date,
    p.end_date::date as parent_end_date,
    delivery.source_received_at::timestamp_ntz as source_received_at,
    s.observed_at::timestamp_ntz as clinical_record_at,
    null::varchar as qualifier_code,
    null::varchar as qualifier_name,
    s.observation_value::varchar as result_value,
    s.categorical_value_description::varchar as result_value_name,
    s.observation_value_numeric::number(38,9) as result_value_numeric,
    s.value_parse_status::varchar as result_value_parse_status,
    s.ucum_unit_code::varchar as result_unit_code,
    s.unit_description::varchar as result_unit_name,
    s.resolved_unit_symbol::varchar as result_unit_symbol,
    null::varchar as assessment_tool_name,
    null::varchar as assessment_response_status
from {{ ref('fct_sus_uec_observation') }} as s
left join {{ ref('obt_encounter_uec') }} as p on s.visit_occurrence_id = p.visit_occurrence_id
left join {{ ref('stg_sus_ecds_emergency_care') }} as delivery on s.visit_occurrence_id = delivery.primarykey_id
where true
{{ navigation_delivery_filter('delivery.source_received_at', 'observation') }}
union all
select
    s.sk_patient_id::varchar as sk_patient_id,
    'ECDS'::varchar as source_dataset,
    'scored_assessment'::varchar as source_record_type,
    s.assessment_id::varchar as source_record_id,
    'fct_sus_uec_scored_assessment'::varchar as source_model_name,
    'scored_assessment'::varchar as clinical_record_type,
    s.validated_at::date as clinical_record_date,
    iff(s.validated_at is null, 'unknown', 'timestamp')::varchar as clinical_time_precision,
    iff(s.validated_at is null, 'not_recorded', 'validated_at')::varchar as clinical_time_basis,
    s.assessment_tool_code::varchar as source_code,
    s.assessment_description::varchar as source_code_name,
    'SNOMED CT'::varchar as source_coding_system,
    s.organisation_id::varchar as provider_organisation_code,
    s.organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    s.visit_occurrence_id::varchar as parent_record_id,
    'emergency_care_attendance'::varchar as parent_record_type,
    'obt_encounter_uec'::varchar as parent_model_name,
    'recorded_parent'::varchar as relationship_type,
    null::boolean as is_primary_diagnosis,
    null::number as coding_position,
    p.start_date::date as parent_start_date,
    p.end_date::date as parent_end_date,
    delivery.source_received_at::timestamp_ntz as source_received_at,
    s.validated_at::timestamp_ntz as clinical_record_at,
    null::varchar as qualifier_code,
    null::varchar as qualifier_name,
    s.person_score::varchar as result_value,
    null::varchar as result_value_name,
    s.person_score_numeric::number(38,9) as result_value_numeric,
    s.value_parse_status::varchar as result_value_parse_status,
    null::varchar as result_unit_code,
    null::varchar as result_unit_name,
    null::varchar as result_unit_symbol,
    s.assessment_description::varchar as assessment_tool_name,
    iff(s.value_parse_status = 'not_recorded', 'value_missing', 'not_validated')::varchar as assessment_response_status
from {{ ref('fct_sus_uec_scored_assessment') }} as s
left join {{ ref('obt_encounter_uec') }} as p on s.visit_occurrence_id = p.visit_occurrence_id
left join {{ ref('stg_sus_ecds_emergency_care') }} as delivery on s.visit_occurrence_id = delivery.primarykey_id
where true
{{ navigation_delivery_filter('delivery.source_received_at', 'scored_assessment') }}
union all
select
    a.sk_patient_id::varchar as sk_patient_id,
    'ECDS'::varchar as source_dataset,
    i.item_type::varchar as source_record_type,
    i.source_record_id::varchar as source_record_id,
    i.source_model_name::varchar as source_model_name,
    i.item_type::varchar as clinical_record_type,
    i.item_date::date as clinical_record_date,
    case when i.item_date is null then 'unknown' when i.item_time is not null then 'timestamp' else 'date' end::varchar as clinical_time_precision,
    iff(i.item_date is null, 'not_recorded', 'injury_date')::varchar as clinical_time_basis,
    i.code::varchar as source_code,
    i.code_name::varchar as source_code_name,
    'SNOMED CT'::varchar as source_coding_system,
    a.organisation_id::varchar as provider_organisation_code,
    a.organisation_name::varchar as provider_organisation_name,
    'ODS'::varchar as provider_code_authority,
    a.visit_occurrence_id::varchar as parent_record_id,
    'emergency_care_attendance'::varchar as parent_record_type,
    'obt_encounter_uec'::varchar as parent_model_name,
    'recorded_parent'::varchar as relationship_type,
    null::boolean as is_primary_diagnosis,
    null::number as coding_position,
    a.start_date::date as parent_start_date,
    a.end_date::date as parent_end_date,
    a.source_received_at::timestamp_ntz as source_received_at,
    timestamp_ntz_from_parts(i.item_date::date, i.item_time::time)::timestamp_ntz as clinical_record_at,
    null::varchar as qualifier_code,
    null::varchar as qualifier_name,
    null::varchar as result_value,
    null::varchar as result_value_name,
    null::number(38,9) as result_value_numeric,
    null::varchar as result_value_parse_status,
    null::varchar as result_unit_code,
    null::varchar as result_unit_name,
    null::varchar as result_unit_symbol,
    null::varchar as assessment_tool_name,
    null::varchar as assessment_response_status
from attendance_items as i
inner join attendance as a on i.visit_occurrence_id = a.visit_occurrence_id
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
    coalesce(source_snomed.preferred_term, source_code_name) as source_code_name,
    source_coding_system,
    source_snomed.snomed_code as mapped_code,
    source_snomed.preferred_term as mapped_code_name,
    iff(source_snomed.snomed_code is not null, 'SNOMED CT', null)::varchar as mapped_coding_system,
    provider_organisation_code,
    provider_organisation_name,
    provider_code_authority,
    parent_record_id,
    parent_record_type,
    parent_model_name,
    relationship_type,
    source_received_at,
    clinical_record_at,
    qualifier_code,
    qualifier_name,
    result_value,
    result_value_name,
    result_value_numeric,
    result_value_parse_status,
    result_unit_code,
    result_unit_name,
    result_unit_symbol,
    assessment_tool_name,
    assessment_response_status,
    is_primary_diagnosis,
    coding_position,
    parent_start_date,
    parent_end_date
from clinical_records
left join {{ ref('snomed_concept') }} as source_snomed
    on trim(clinical_records.source_code) = source_snomed.snomed_code
