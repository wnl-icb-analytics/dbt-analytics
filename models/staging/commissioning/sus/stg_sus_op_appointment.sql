{{
    config(materialized = 'view')
}}

-- Deduplicate the lookup before joining so dictionary versions cannot multiply appointments.
with organisation_codes as (
    select distinct organisation_code
    from {{ ref('stg_dictionary_dbo_organisation') }}
    where sk_organisation_type_id = 41
)

select  primarykey_id,

    -- patient details at time
    appointment_patient_identity_nhs_number_value_pseudo as sk_patient_id,
    appointment_patient_identity_local_patient_identifier_value as local_patient_identifier,
    appointment_patient_residence_derived_postcode_district,
    appointment_patient_residence_derived_lsoa_11,
    appointment_patient_residence_derived_local_authority_district,
    appointment_patient_identity_ethnic_category_2021,
    appointment_patient_residence_derived_index_of_multiple_deprivation_decile,
    appointment_patient_residence_derived_index_of_multiple_deprivation_decile_description,
    appointment_patient_identity_gender,
    appointment_patient_identity_ethnic_category,
    appointment_patient_registration_general_practice,
    appointment_patient_identity_birth_year,
    appointment_patient_identity_birth_month,
    appointment_patient_identity_age_at_cds_activity_date,

    -- appointment information
    appointment_identifier, 
    appointment_date::date as appointment_date,
    appointment_time::time as appointment_time,
    case when provider.organisation_code is not null
        then core.appointment_commissioning_service_agreement_provider
        else left(core.appointment_commissioning_service_agreement_provider, 3)
    end as appointment_commissioning_service_agreement_provider,
    appointment_care_location_site_code_of_treatment,
    appointment_expected_duration::integer as appointment_expected_duration,
    appointment_outcome,
    appointment_attended_or_dna,
    appointment_first_attendance,
    appointment_administrative_category,
    appointment_care_professional_main_specialty,
    appointment_referral_referring_organisation,
    appointment_referral_received_date::date as appointment_referral_received_date,
    appointment_referral_source_op,
    appointment_referral_priority_type,
    appointment_care_professional_treatment_function,
    appointment_commissioning_grouping_core_hrg,
    //Added Spec Comm
    appointment_commissioning_pss_grouping_national_programme_code as spec_comm,
    appointment_commissioning_tariff_calculation_final_price
from {{ ref('raw_sus_op_appointment') }} as core
left join organisation_codes as provider
    on core.appointment_commissioning_service_agreement_provider = provider.organisation_code
