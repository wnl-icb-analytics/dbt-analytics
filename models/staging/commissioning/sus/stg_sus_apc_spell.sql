{{
    config(materialized = 'table')
}}

with core_data as(
    select *
    from {{ ref('raw_sus_apc_spell') }} as core
    qualify row_number() over (
        partition by primarykey_id
        order by
            system_transaction_cds_activity_date desc nulls last
            , try_to_number(system_record_version) desc nulls last
            , system_interchange_received_date desc nulls last
            , system_interchange_received_time desc nulls last
            , rownumber_id desc
        ) = 1
)

select core.primarykey_id
    , {{ consistent_sk_patient_id_format('core.spell_patient_identity_nhs_number_value_pseudo') }} as sk_patient_id
    , spell_patient_identity_local_patient_identifier_value as local_patient_identifier

    /* spell details */
    -- location
    , {{ clean_organisation_id('SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER') }} as SPELL_COMMISSIONING_SERVICE_AGREEMENT_PROVIDER
    , core.spell_care_location_site_code_of_treatment

     /* Time and date */
    , core.spell_admission_date::date as spell_admission_date 
    , core.spell_admission_time::time as spell_admission_time
    , core.spell_discharge_date::date as spell_discharge_date
    , core.spell_discharge_time::time as spell_discharge_time
    , core.spell_open_spell_indicator
    , core.spell_discharge_length_of_hospital_stay
    , core.spell_commissioning_tariff_calculation_pbr_length_of_stay_unadjusted_days
    , core.spell_commissioning_tariff_calculation_pbr_length_of_stay_critical_care_days as spell_length_of_stay_critical_care_days
    , core.spell_commissioning_tariff_calculation_pbr_length_of_stay_excess_bed_days
    , core.spell_discharge_destination
    , core.spell_discharge_method

    /* Admission information */
    , core.spell_admission_method
    , core.spell_admission_admission_type
    , core.spell_admission_admission_sub_type
    , core.spell_admission_patient_classification

    -- clinical information
    , {{clean_icd10_code("spell_clinical_coding_grouper_derived_primary_diagnosis")}} as spell_clinical_coding_grouper_derived_primary_diagnosis
    , {{clean_icd10_code("core.spell_clinical_coding_grouper_derived_secondary_diagnosis")}} as spell_clinical_coding_grouper_derived_secondary_diagnosis
    , core.spell_clinical_coding_grouper_derived_dominant_procedure
    , core.spell_patient_identity_pds_date_of_death
    , core.spell_patient_identity_pds_death_status

    -- commissioning and costing details
    , core.spell_commissioning_grouping_core_hrg
    , core.spell_commissioning_tariff_calculation_final_price
    , core.spell_commissioning_pss_grouping_national_programme_code -- spec comm

    /* patient info at time of event  */
    , core.spell_patient_residence_derived_postcode_district
    , core.spell_patient_residence_derived_lsoa_11
    , core.spell_patient_residence_residence_ccg
    , core.spell_patient_residence_derived_local_authority_district
    , core.spell_patient_residence_derived_index_of_multiple_deprivation_decile
    , core.spell_patient_residence_derived_index_of_multiple_deprivation_decile_description
    , core.spell_patient_identity_age_on_admission
    , core.spell_patient_identity_gender
    , core.spell_patient_identity_ethnic_category
    , core.spell_patient_registration_general_practice
    , core.spell_patient_identity_spell_age
    , core.spell_patient_identity_birth_year
    , core.spell_patient_identity_birth_month

from core_data as core


