/*
Inpatient encounters from SUS

Clinical Purpose:
- Establishing use of inpatient services
- Understanding patient service preference
- Care coordination management across providers

One row per staged spell, including open spells. No additional date or patient-status filter.

*/
with ethnicity_codes as (
    select distinct bk_ethnicity_code, ethnicity_desc 
    from {{ref('stg_dictionary_dbo_ethnicity')}}
    where ethnicity_code_type = 'Current' or bk_ethnicity_code = '99'
    ),
gender_codes as (
    select distinct gender_code, gender
    from {{ref('stg_dictionary_dbo_gender')}}
    ),

dominant_episode_information as (
    select 
        primarykey_id,
        care_professional_main_specialty,
        care_professional_treatment_function
    from {{ ref('stg_sus_apc_spell_episodes') }}
    where dominant_episode_flag = '1'
    qualify row_number() over (
        partition by primarykey_id 
        order by episodes_id -- TODO: check if this is correct, if multiple dominant episodes, should we take the first?
    ) = 1
)
select 
    /* Information needed to derive standard encounter information */
    core.primarykey_id as visit_occurrence_id
    , core.sk_patient_id
    , 'SUS_APC' as source
    , core.local_patient_identifier

    /* Location */
    , core.spell_commissioning_service_agreement_provider as organisation_id
    , dict_provider.service_provider_name  as organisation_name
    , core.spell_care_location_site_code_of_treatment as site_id
    , dict_site.organisation_name as site_name
    
    /* Time and date */
    , core.spell_admission_date as start_date
    , core.spell_admission_time as start_time
    , core.spell_discharge_date as end_date
    , core.spell_discharge_time as end_time
    , core.spell_open_spell_indicator = 1 as is_open_spell
    , core.spell_discharge_length_of_hospital_stay as duration
    , datediff(day, core.spell_admission_date, coalesce(core.spell_discharge_date, current_date)) as duration_to_date -- inefficient? Change to calc only if no end date?
    , core.spell_commissioning_tariff_calculation_pbr_length_of_stay_unadjusted_days as unadjusted_length_of_stay_days
    , core.spell_commissioning_tariff_calculation_pbr_length_of_stay_excess_bed_days as excess_bed_days
   
    /* Admission information */
    , core.spell_admission_method as spell_admission_method 
    , dict_adm_method.admission_method_name as admission_method_name
    , dict_adm_method.admission_method_group as admission_method_group
    , dict_patient_class.patient_classification_name as admission_patient_classification
    , case 
        when core.spell_admission_method in ('11', '12', '13') -- dict_adm_method.admission_method_group = 'Elective'
            and core.spell_admission_patient_classification = '2' -- dict_patient_class.patient_classification_name = 'Day case admission'
            then 'DC' -- Day Case
        when core.spell_admission_method in ('11', '12', '13') -- dict_adm_method.admission_method_group = 'Elective'
            and core.spell_admission_patient_classification = '1' -- dict_patient_class.patient_classification_name = 'Ordinary admission'
            then 'EL' -- Elective
        when core.spell_admission_method in ('11', '12', '13') --  dict_adm_method.admission_method_group = 'Elective'
            and core.spell_admission_patient_classification in ('3', '4') -- dict_patient_class.patient_classification_name in ('Regular day admission', 'Regular night admission')
            then 'RA' -- Regular Attender (day & night)
        when core.spell_admission_method in ('21', '22', '23', '24', '25', '28','2A','2B','2C','2D') -- dict_adm_method.admission_method_group = 'Non-elective - emergency'
            and datediff(day, core.spell_admission_date, coalesce(core.spell_discharge_date, current_date)) = 0 
            then 'NEL-ZLOS'
        when core.spell_admission_method in ('21', '22', '23', '24', '25', '28','2A','2B','2C','2D') -- dict_adm_method.admission_method_group = 'Non-elective - emergency'
            and datediff(day, core.spell_admission_date, coalesce(core.spell_discharge_date, current_date)) >= 1 
            then 'NEL-LOS+1'
        when core.spell_admission_method in ('31', '32','82', '83') -- dict_adm_method.admission_method_group in ('Non-elective - Maternity') or dict_adm_method.admission_method_name in ('The birth of a baby', 'Baby born outside the Provider')
            then 'NELNE'
        when core.spell_admission_method = '81' -- dict_adm_method.admission_method_name = 'Transfer'
            then 'TRANSFERS'
        else 'OTHER' end as pod
    
    /* Discharge information */
    , core.spell_discharge_destination as discharge_destination_code
    , dict_discharge_destination.description as discharge_destination_name
    , core.spell_discharge_method as discharge_method_code
    , dict_discharge_method.description as discharge_method_name
    -- SUS+ derives these days from submitted critical-care activity; dbt passes them through.
    -- Do not subtract the length-of-stay adjustment again from duration.
    , core.spell_length_of_stay_critical_care_days as critical_care_days_for_length_of_stay
    
    /* Clinical information */
    , core.spell_clinical_coding_grouper_derived_primary_diagnosis  as primary_diagnosis_code
    , core.spell_clinical_coding_grouper_derived_secondary_diagnosis  as secondary_diagnosis_code
    , core.spell_clinical_coding_grouper_derived_dominant_procedure as primary_treatment

    /* Clinician information */
    , dom_ep_info.care_professional_main_specialty as main_specialty_code
    , dict_spec.specialty_name as main_specialty_name
    , dict_spec.specialty_category as main_specialty_category
    , dom_ep_info.care_professional_treatment_function as treatment_function_code
    , dict_treat.specialty_name as treatment_function_code_desc

    /* Commissioning information */
    , core.spell_commissioning_grouping_core_hrg as hrg_code
    , dict_hrg.hrg_description as core_hrg_desc
    , dict_hrg.hrg_chapter_key as core_hrg_chapter
    , dict_hrg.hrg_chapter as core_hrg_chapter_desc
    , iff(core.spell_commissioning_pss_grouping_national_programme_code is null, 'N','Y') as spec_comm_flag
    , core.spell_commissioning_pss_grouping_national_programme_code as spec_comm
    , iff(core.spell_admission_admission_sub_type = 'NON', core.spell_admission_admission_type, core.spell_admission_admission_sub_type) as type
    , core.spell_commissioning_tariff_calculation_final_price as cost
    
    /* patient info at time of event  */
    , core.spell_patient_identity_spell_age as age_at_event
    , core.spell_patient_identity_gender as gender_at_event
    , gen.gender as gender_desc_at_event
    , core.spell_patient_identity_ethnic_category as ethnicity_at_event
    , eth.ethnicity_desc as ethnicity_desc_at_event 
    , core.spell_patient_residence_derived_postcode_district as postcode_district_at_event
    , core.spell_patient_residence_derived_lsoa_11 as lsoa_11_at_event
    , core.spell_patient_residence_derived_local_authority_district as lad_at_event
    , core.spell_patient_residence_derived_index_of_multiple_deprivation_decile as imd_at_event
    , core.spell_patient_registration_general_practice as reg_practice_at_event
    , 'APC_SPELL' as visit_occurrence_type


from {{ ref('stg_sus_apc_spell')}} as core

left join {{ ref('stg_dictionary_ip_admissionmethods')}} as dict_adm_method
    ON core.spell_admission_method = dict_adm_method.bk_admission_method_code

left join {{ ref('discharge_destination') }} as dict_discharge_destination
    on core.spell_discharge_destination = dict_discharge_destination.code

left join {{ ref('discharge_method') }} as dict_discharge_method
    on core.spell_discharge_method = dict_discharge_method.code

left join {{ ref('stg_dictionary_dbo_patientclassification')}} as dict_patient_class
    ON core.spell_admission_patient_classification = dict_patient_class.bk_patient_classification_code

left join ethnicity_codes as eth
    on core.spell_patient_identity_ethnic_category = eth.bk_ethnicity_code

left join gender_codes as gen
    on core.spell_patient_identity_gender = gen.gender_code

LEFT JOIN {{ ref('stg_dictionary_dbo_serviceprovider') }} as dict_provider 
    ON core.SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER = dict_provider.service_provider_full_code

-- site name: site-level treatment code resolves against the full organisation
-- dictionary (hospital sites are type 42; organisation_nhs_provider is trusts
-- only, type 41, so it left site_name null). Mirrors int_sus_uec_encounter.
LEFT JOIN {{ ref('stg_dictionary_dbo_organisation') }} as dict_site
    ON core.spell_care_location_site_code_of_treatment = dict_site.organisation_code

left join dominant_episode_information as dom_ep_info
    ON core.primarykey_id = dom_ep_info.primarykey_id

LEFT JOIN  {{ ref('stg_dictionary_dbo_specialties')}} as dict_spec
    ON  dom_ep_info.care_professional_main_specialty = dict_spec.bk_specialty_code
    and dict_spec.is_main_specialty = TRUE 

left join {{ref('stg_dictionary_dbo_specialties')}} as dict_treat 
    on dom_ep_info.care_professional_treatment_function = dict_treat.bk_specialty_code 
    and dict_treat.is_treatment_function = TRUE

left join
    {{ ref('stg_dictionary_dbo_hrg') }} as dict_hrg 
    on core.spell_commissioning_grouping_core_hrg = dict_hrg.hrg_code
