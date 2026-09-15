/*
Outpatient appointments from SUS

Processing:
- Select key fields
- Rename fields to standard encounter model
- Add dictionary lookups to int_sus_op_min to provide descriptive fields
- Map to known definitions [added later]
- Keeps all appointments

Clinical Purpose:
- Establishing use of outpatient services
- Understanding patient service preference
- Care coordination management across providers

Includes ALL persons (active, inactive, deceased) within 5 years following intermediate layer principles.

*/

with 
ethnicity_codes as (
    select distinct bk_ethnicity_code, ethnicity_desc 
    from {{ref('stg_dictionary_dbo_ethnicity')}}
    where ethnicity_code_type = 'Current' or bk_ethnicity_code = '99'
    ),
gender_codes as (
    select distinct gender_code, gender
    from {{ref('stg_dictionary_dbo_gender')}}
    )

select 
    /* Information needed to derive standard encounter information */
    core.primarykey_id as visit_occurrence_id
    , core.sk_patient_id
    , 'SUS_OP' as source
    , core.local_patient_identifier

    /* Location */
    , core.appointment_commissioning_service_agreement_provider as organisation_id
    , dict_provider.service_provider_name as organisation_name
    , core.appointment_care_location_site_code_of_treatment as site_id
    , dict_site.organisation_name as site_name

     /* Time and date */
    , core.appointment_date as start_date
    , core.appointment_time as start_time
    , core.appointment_expected_duration as expected_duration
    
    /* Outcome information */
    , core.appointment_outcome as appointment_outcome
    , dict_appt_outcome.attendance_outcome as outcome_desc
    , core.appointment_attended_or_dna as appointment_attended_or_dna
    , dict_att_dna.dna_indicator_desc as appointment_attendance_outcome_desc

    /* Appointment information */
    , core.appointment_first_attendance as appointment_first_attendance
    , dict_att_t.attendant_type_desc as first_attendance_desc
    , case
        when core.appointment_commissioning_grouping_core_hrg in ('WF01A','WF02A') then 'OPFUP-F2F'
        when core.appointment_commissioning_grouping_core_hrg in ('WF01B','WF02B') then 'OPFA-F2F'
        when core.appointment_commissioning_grouping_core_hrg in ('WF01C','WF02C') then 'OPFUP-NFTF'
        when core.appointment_commissioning_grouping_core_hrg in ('WF01D','WF02D') then 'OPFA-NFTF'
        when left(core.appointment_commissioning_grouping_core_hrg, 1) not in ('W', 'U')
            and dict_att_t.attendant_type_desc = 'First' then 'OPPROC-FA'
        when left(core.appointment_commissioning_grouping_core_hrg, 1) not in ('W', 'U')
            and dict_att_t.attendant_type_desc = 'Follow-up' then 'OPPROC-FU'
        when core.appointment_commissioning_grouping_core_hrg is null then 'UNKNOWN'
        else 'OPPROC'
        end as pod

    /* Clinical */
    ,diag.code AS primary_diagnosis_code
    ,dict_diag.description AS primary_diagnosis_name
    ,proc.code  AS primary_procedure_code
    ,dict_proc.category AS primary_procedure_category
    ,dict_proc.description AS primary_procedure_name

    /* Clinician information */
    , core.appointment_care_professional_main_specialty as main_specialty_code
    , dict_spec.specialty_name as main_specialty_name
    , dict_spec.specialty_category as main_specialty_category
    , core.appointment_care_professional_treatment_function as treatment_function_code
    , dict_treat.specialty_name as treatment_function_code_desc

    /* Referral information */
    , core.appointment_referral_referring_organisation as referral_organisation_id
    , core.appointment_referral_received_date as referral_organisation_date
    , core.appointment_referral_source_op as referral_source_op
    , dict_appt_referral_source.referral_group as referral_source_op_desc
    , core.appointment_referral_priority_type as referral_acuity -- proxy for acuity, change as poor
    , dict_appt_priority.priority_type_desc as referral_acuity_desc

    /* Commissioning information */
    , iff(core.spec_comm_code is null, 'N','Y') as spec_comm_flag -- Adding Spec_comm   
    , core.spec_comm_code as spec_comm_code
    , dict_pss_sc.spec_comm_desc
    , core.pss_service_line_code as pss_service_line_code
    , dict_pss_sl.service_line_number_desc as pss_service_line_desc
    , core.appointment_commissioning_grouping_core_hrg as core_hrg_code -- consider changing to something more aligned with team understanding
    , dict_hrg.hrg_description as core_hrg_desc
    , dict_hrg.hrg_chapter_key as core_hrg_chapter
    , dict_hrg.hrg_chapter as core_hrg_chapter_desc
    , core.appointment_commissioning_tariff_calculation_final_price as cost
    
    /* Patient details at time */
    ,core.appointment_patient_identity_age_at_cds_activity_date as age_at_event
    ,core.appointment_patient_identity_gender as gender_at_event
    ,gen.gender as gender_desc_at_event
    ,core.appointment_patient_identity_ethnic_category as ethnicity_at_event
    ,eth.ethnicity_desc as ethnicity_desc_at_event
    ,core.appointment_patient_residence_derived_postcode_district as postcode_district_at_event
    ,core.appointment_patient_residence_derived_lsoa_11 as lsoa_11_at_event
    ,core.appointment_patient_residence_derived_local_authority_district as lad_at_event
    ,core.appointment_patient_residence_derived_index_of_multiple_deprivation_decile as imd_at_event
    ,core.appointment_patient_registration_general_practice as reg_practice_at_event
    ,'OP_ATTENDANCE' as visit_occurrence_type

from {{ ref('stg_sus_op_appointment')}} as core -- TO DO: check if appointments and encounters can be used interchangeably in this context

-- speciality and treatment descriptions
left join 
    {{ref('stg_dictionary_dbo_specialties')}} as dict_spec 
    on core.appointment_care_professional_main_specialty = dict_spec.bk_specialty_code 
    and dict_spec.is_main_specialty = TRUE

left join 
    {{ref('stg_dictionary_dbo_specialties')}} as dict_treat 
    on core.appointment_care_professional_treatment_function = dict_treat.bk_specialty_code 
    and dict_treat.is_treatment_function = TRUE

left join
    {{ ref('stg_dictionary_dbo_hrg') }} as dict_hrg 
    on core.appointment_commissioning_grouping_core_hrg = dict_hrg.hrg_code

-- diagnoses and procedures
LEFT JOIN {{ ref("stg_sus_op_appointment_clinical_coding_diagnosis_icd") }} diag on core.PRIMARYKEY_ID = diag.PRIMARYKEY_ID and diag.ICD_ID = 1 
LEFT JOIN {{ ref('stg_dictionary_dbo_diagnosis')}} As dict_diag ON diag.code = dict_diag.code

-- need to stage diagnosis dictionary to get description
LEFT JOIN {{ ref("stg_sus_op_appointment_clinical_coding_procedure_opcs") }} proc on core.PRIMARYKEY_ID = proc.PRIMARYKEY_ID and proc.OPCS_ID = 1 
LEFT JOIN {{ ref('stg_dictionary_dbo_procedure')}} As dict_proc ON proc.code = dict_proc.code
-- need to stage procedure dictionary to get description and category

-- organisations
-- site name: site-level treatment code resolves against the full organisation
-- dictionary (hospital sites are type 42; organisation_nhs_provider is trusts
-- only, type 41, so it left site_name null). Mirrors int_sus_uec_encounter.
left join
    {{ ref('stg_dictionary_dbo_organisation') }} as dict_site
    on core.appointment_care_location_site_code_of_treatment = dict_site.organisation_code

left join 
    {{ ref('stg_dictionary_dbo_serviceprovider') }} as dict_provider 
    on core.appointment_commissioning_service_agreement_provider = dict_provider.service_provider_full_code

-- referral source
left join
    {{ ref('stg_dictionary_op_sourceofreferrals') }} as dict_appt_referral_source
    on core.appointment_referral_source_op = dict_appt_referral_source.bk_source_of_referral_code

-- referral priority
left join
    {{ ref('stg_dictionary_op_prioritytype') }} as dict_appt_priority 
    on cast(core.appointment_referral_priority_type as bigint) = dict_appt_priority.bk_priority_type_code

-- attendance outcome
left join {{ ref('stg_dictionary_op_attendanceoutcomes') }} as dict_appt_outcome 
    on core.appointment_outcome = dict_appt_outcome.bk_attendance_outcome

left join {{ ref('stg_dictionary_op_dnaindicators') }} as dict_att_dna 
    on core.appointment_attended_or_dna = dict_att_dna.bk_dna_code

left join {{ ref('stg_dictionary_op_attendancetypes') }} as dict_att_t 
    on core.appointment_first_attendance = dict_att_t.bk_attendance_type_code

left join ethnicity_codes as eth
    on core.appointment_patient_identity_ethnic_category = eth.bk_ethnicity_code

left join gender_codes as gen
    on core.appointment_patient_identity_gender = gen.gender_code

left join
    {{ ref('pss_spec_comm')}} as dict_pss_sc
    on core.spec_comm_code = dict_pss_sc.spec_comm_code

left join
    {{ ref('pss_service_line')}} as dict_pss_sl
    on core.pss_service_line_code = dict_pss_sl.service_line_number_code