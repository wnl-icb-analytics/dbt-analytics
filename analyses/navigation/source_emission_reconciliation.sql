-- One aggregate per output/dataset/detail model/emitted type. Run on deployment
-- or an adapter change, not daily. Primary events retain undated rows; optional
-- milestones need their supplied date. OLIDS reconciles the prepared snapshot.
with expected as (
    select 'healthcare_event' as output_type, 'MHSDS' as source_dataset,
        'fct_mhsds_referral' as source_model_name, 'referral_received' as emitted_type, count(*) as expected_record_count
    from {{ ref('fct_mhsds_referral') }}
    union all
    select 'healthcare_event' as output_type, 'MHSDS' as source_dataset,
        'fct_mhsds_referral' as source_model_name, 'referral_rejected' as emitted_type, count(*) as expected_record_count
    from {{ ref('fct_mhsds_referral') }} where referral_rejected_at is not null
    union all
    select 'healthcare_event' as output_type, 'MHSDS' as source_dataset,
        'fct_mhsds_referral' as source_model_name, 'referral_discharged' as emitted_type, count(*) as expected_record_count
    from {{ ref('fct_mhsds_referral') }} where referral_discharge_date is not null
    union all
    select 'healthcare_event' as output_type, 'MHSDS' as source_dataset,
        'fct_mhsds_care_contact' as source_model_name, 'care_contact' as emitted_type, count(*) as expected_record_count
    from {{ ref('fct_mhsds_care_contact') }}
    union all
    select 'healthcare_event' as output_type, 'MHSDS' as source_dataset,
        'fct_mhsds_hospital_provider_spell' as source_model_name, 'admission' as emitted_type, count(*) as expected_record_count
    from {{ ref('fct_mhsds_hospital_provider_spell') }}
    union all
    select 'healthcare_event' as output_type, 'MHSDS' as source_dataset,
        'fct_mhsds_hospital_provider_spell' as source_model_name, 'discharge' as emitted_type, count(*) as expected_record_count
    from {{ ref('fct_mhsds_hospital_provider_spell') }} where discharge_date is not null
    union all
    select 'healthcare_event' as output_type, 'MHSDS' as source_dataset,
        'fct_mhsds_ward_stay' as source_model_name, 'ward_stay_started' as emitted_type, count(*) as expected_record_count
    from {{ ref('fct_mhsds_ward_stay') }}
    union all
    select 'healthcare_event' as output_type, 'MHSDS' as source_dataset,
        'fct_mhsds_ward_stay' as source_model_name, 'ward_stay_ended' as emitted_type, count(*) as expected_record_count
    from {{ ref('fct_mhsds_ward_stay') }} where ward_stay_end_date is not null
    union all
    select 'healthcare_event' as output_type, 'CSDS' as source_dataset,
        'fct_csds_referral' as source_model_name, 'referral_received' as emitted_type, count(*) as expected_record_count
    from {{ ref('fct_csds_referral') }}
    union all
    select 'healthcare_event' as output_type, 'CSDS' as source_dataset,
        'fct_csds_referral' as source_model_name, 'referral_discharged' as emitted_type, count(*) as expected_record_count
    from {{ ref('fct_csds_referral') }} where service_discharge_date is not null
    union all
    select 'healthcare_event' as output_type, 'CSDS' as source_dataset,
        'fct_csds_care_contact' as source_model_name, 'care_contact' as emitted_type, count(*) as expected_record_count
    from {{ ref('fct_csds_care_contact') }}
    union all
    select 'healthcare_event' as output_type, 'SUS_APC' as source_dataset,
        'obt_encounter_apc' as source_model_name, 'admission' as emitted_type, count(*) as expected_record_count
    from {{ ref('obt_encounter_apc') }}
    union all
    select 'healthcare_event' as output_type, 'SUS_APC' as source_dataset,
        'obt_encounter_apc' as source_model_name, 'discharge' as emitted_type, count(*) as expected_record_count
    from {{ ref('obt_encounter_apc') }} where end_date is not null
    union all
    select 'healthcare_event' as output_type, 'SUS_APC' as source_dataset,
        'obt_encounter_apc' as source_model_name, 'inpatient_decision_to_admit' as emitted_type, count(*) as expected_record_count
    from {{ ref('obt_encounter_apc') }} where decided_to_admit_date is not null
    union all
    select 'healthcare_event' as output_type, 'SUS_APC' as source_dataset,
        'obt_encounter_apc' as source_model_name, 'discharge_ready' as emitted_type, count(*) as expected_record_count
    from {{ ref('obt_encounter_apc') }} where discharge_ready_date is not null
    union all
    select 'healthcare_event' as output_type, 'SUS_OP' as source_dataset,
        'int_sus_op_appointment' as source_model_name, 'appointment_slot' as emitted_type, count(*) as expected_record_count
    from {{ ref('int_sus_op_appointment') }}
    union all
    select 'healthcare_event' as output_type, 'ECDS' as source_dataset,
        'obt_encounter_uec' as source_model_name, 'emergency_care_arrival' as emitted_type, count(*) as expected_record_count
    from {{ ref('obt_encounter_uec') }}
    union all
    select 'healthcare_event' as output_type, 'ECDS' as source_dataset,
        'obt_encounter_uec' as source_model_name, 'initial_assessment' as emitted_type, count(*) as expected_record_count
    from {{ ref('obt_encounter_uec') }} where initial_assessment_date is not null
    union all
    select 'healthcare_event' as output_type, 'ECDS' as source_dataset,
        'obt_encounter_uec' as source_model_name, 'seen_for_treatment' as emitted_type, count(*) as expected_record_count
    from {{ ref('obt_encounter_uec') }} where seen_for_treatment_date is not null
    union all
    select 'healthcare_event' as output_type, 'ECDS' as source_dataset,
        'obt_encounter_uec' as source_model_name, 'decision_to_admit' as emitted_type, count(*) as expected_record_count
    from {{ ref('obt_encounter_uec') }} where decided_to_admit_date is not null
    union all
    select 'healthcare_event' as output_type, 'ECDS' as source_dataset,
        'obt_encounter_uec' as source_model_name, 'emergency_care_departure' as emitted_type, count(*) as expected_record_count
    from {{ ref('obt_encounter_uec') }} where end_date is not null
    union all
    select 'healthcare_event', 'ERS', 'fct_ers_referral_action', m.event_type, count(*)
    from {{ ref('fct_ers_referral_action') }} as a
    join {{ ref('ers_healthcare_event_type') }} as m on a.action_code = m.action_code
    group by m.event_type
    union all
    select 'healthcare_event', 'ERS', 'fct_ers_appointment', 'appointment_slot', count(*)
    from {{ ref('fct_ers_appointment') }} as a
    join {{ ref('fct_ers_referral_action') }} as latest on a.latest_action_id = latest.action_id
    union all
    select 'healthcare_event', 'OLIDS',
        case source_record_type when 'appointment' then 'fct_gp_appointment'
            when 'referral_request' then 'fct_gp_referral_request' end,
        event_type, count(*)
    from {{ ref('fct_gp_healthcare_event') }}
    group by 3, event_type
    union all
    select 'clinical_record', 'MHSDS', 'fct_mhsds_clinical_record', clinical_record_type, count(*)
    from {{ ref('fct_mhsds_clinical_record') }} group by clinical_record_type
    union all
    select 'clinical_record', 'CSDS', 'fct_csds_clinical_record', clinical_record_type, count(*)
    from {{ ref('fct_csds_clinical_record') }} group by clinical_record_type
    union all
    select 'clinical_record' as output_type, 'SUS_APC' as source_dataset,
        'stg_sus_apc_spell_episodes_clinical_coding_diagnosis_icd' as source_model_name, 'diagnosis' as emitted_type, count(*) as expected_record_count
    from {{ ref('stg_sus_apc_spell_episodes_clinical_coding_diagnosis_icd') }}
    union all
    select 'clinical_record' as output_type, 'SUS_APC' as source_dataset,
        'stg_sus_apc_spell_episodes_clinical_coding_procedure_opcs' as source_model_name, 'procedure' as emitted_type, count(*) as expected_record_count
    from {{ ref('stg_sus_apc_spell_episodes_clinical_coding_procedure_opcs') }}
    union all
    select 'clinical_record' as output_type, 'SUS_OP' as source_dataset,
        'stg_sus_op_appointment_clinical_coding_diagnosis_icd' as source_model_name, 'diagnosis' as emitted_type, count(*) as expected_record_count
    from {{ ref('stg_sus_op_appointment_clinical_coding_diagnosis_icd') }}
    union all
    select 'clinical_record' as output_type, 'SUS_OP' as source_dataset,
        'stg_sus_op_appointment_clinical_coding_procedure_opcs' as source_model_name, 'procedure' as emitted_type, count(*) as expected_record_count
    from {{ ref('stg_sus_op_appointment_clinical_coding_procedure_opcs') }}
    union all
    select 'clinical_record' as output_type, 'ECDS' as source_dataset,
        'stg_sus_ecds_clinical_diagnoses_snomed' as source_model_name, 'diagnosis' as emitted_type, count(*) as expected_record_count
    from {{ ref('stg_sus_ecds_clinical_diagnoses_snomed') }}
    union all
    select 'clinical_record' as output_type, 'ECDS' as source_dataset,
        'stg_sus_ecds_clinical_treatments_snomed' as source_model_name, 'treatment' as emitted_type, count(*) as expected_record_count
    from {{ ref('stg_sus_ecds_clinical_treatments_snomed') }}
    union all
    select 'clinical_record' as output_type, 'ECDS' as source_dataset,
        'stg_sus_ecds_clinical_investigations_snomed' as source_model_name, 'investigation' as emitted_type, count(*) as expected_record_count
    from {{ ref('stg_sus_ecds_clinical_investigations_snomed') }}
    union all
    select 'clinical_record' as output_type, 'ECDS' as source_dataset,
        'stg_sus_ecds_clinical_comorbidities' as source_model_name, 'comorbidity' as emitted_type, count(*) as expected_record_count
    from {{ ref('stg_sus_ecds_clinical_comorbidities') }}
    union all
    select 'clinical_record' as output_type, 'ECDS' as source_dataset,
        'stg_sus_ecds_clinical_coded_findings' as source_model_name, 'clinical_finding' as emitted_type, count(*) as expected_record_count
    from {{ ref('stg_sus_ecds_clinical_coded_findings') }}
    union all
    select 'clinical_record', 'OLIDS', 'fct_gp_clinical_record', source_record_type, count(*)
    from {{ ref('fct_gp_clinical_record') }} group by source_record_type
), actual as (
    select 'healthcare_event' as output_type, source_dataset, source_model_name,
        event_type as emitted_type, count(*) as actual_record_count
    from {{ ref('int_mhsds_healthcare_event') }}
    group by source_dataset, source_model_name, event_type
    union all
    select 'healthcare_event' as output_type, source_dataset, source_model_name,
        event_type as emitted_type, count(*) as actual_record_count
    from {{ ref('int_csds_healthcare_event') }}
    group by source_dataset, source_model_name, event_type
    union all
    select 'healthcare_event' as output_type, source_dataset, source_model_name,
        event_type as emitted_type, count(*) as actual_record_count
    from {{ ref('int_olids_healthcare_event') }}
    group by source_dataset, source_model_name, event_type
    union all
    select 'healthcare_event' as output_type, source_dataset, source_model_name,
        event_type as emitted_type, count(*) as actual_record_count
    from {{ ref('int_sus_healthcare_event') }}
    group by source_dataset, source_model_name, event_type
    union all
    select 'healthcare_event' as output_type, source_dataset, source_model_name,
        event_type as emitted_type, count(*) as actual_record_count
    from {{ ref('int_ers_healthcare_event') }}
    group by source_dataset, source_model_name, event_type
    union all
    select 'clinical_record' as output_type, source_dataset, source_model_name,
        clinical_record_type as emitted_type, count(*) as actual_record_count
    from {{ ref('int_mhsds_person_clinical_record') }}
    group by source_dataset, source_model_name, clinical_record_type
    union all
    select 'clinical_record' as output_type, source_dataset, source_model_name,
        clinical_record_type as emitted_type, count(*) as actual_record_count
    from {{ ref('int_csds_person_clinical_record') }}
    group by source_dataset, source_model_name, clinical_record_type
    union all
    select 'clinical_record' as output_type, source_dataset, source_model_name,
        clinical_record_type as emitted_type, count(*) as actual_record_count
    from {{ ref('int_olids_person_clinical_record') }}
    group by source_dataset, source_model_name, clinical_record_type
    union all
    select 'clinical_record' as output_type, source_dataset, source_model_name,
        clinical_record_type as emitted_type, count(*) as actual_record_count
    from {{ ref('int_sus_person_clinical_record') }}
    group by source_dataset, source_model_name, clinical_record_type
    union all
    select 'clinical_record' as output_type, source_dataset, source_model_name,
        clinical_record_type as emitted_type, count(*) as actual_record_count
    from {{ ref('int_ecds_person_clinical_record') }}
    group by source_dataset, source_model_name, clinical_record_type
)
select coalesce(e.output_type, a.output_type) as output_type,
    coalesce(e.source_dataset, a.source_dataset) as source_dataset,
    coalesce(e.source_model_name, a.source_model_name) as source_model_name,
    coalesce(e.emitted_type, a.emitted_type) as emitted_type,
    coalesce(e.expected_record_count, 0) as expected_record_count,
    coalesce(a.actual_record_count, 0) as actual_record_count,
    coalesce(a.actual_record_count, 0) - coalesce(e.expected_record_count, 0) as emission_difference
from expected as e full outer join actual as a
    on e.output_type = a.output_type and e.source_dataset = a.source_dataset
    and equal_null(e.source_model_name, a.source_model_name)
    and equal_null(e.emitted_type, a.emitted_type)
