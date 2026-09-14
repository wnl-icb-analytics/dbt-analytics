with 
ethnicity_codes as (
    select distinct bk_ethnicity_code, ethnicity_desc 
    from {{ref('stg_dictionary_dbo_ethnicity')}}
    where ethnicity_code_type = 'Current' or bk_ethnicity_code = '99'
    ),
gender_codes as (
    select distinct gender_code, gender
    from {{ref('stg_dictionary_dbo_gender')}}
    ),
op_demo as(
    select primarykey_id,
    sk_patient_id,
    appointment_date as code_date,
    appointment_patient_identity_age_at_cds_activity_date as age_at_event,
    appointment_patient_identity_gender as gender_at_event,
    appointment_patient_identity_ethnic_category as ethnicity_at_event,
    appointment_patient_residence_derived_postcode_district as postcode_district_at_event,
    appointment_patient_residence_derived_lsoa_11 as lsoa_11_at_event,
    appointment_patient_residence_derived_local_authority_district as lad_at_event,
    appointment_patient_residence_derived_index_of_multiple_deprivation_decile as imd_at_event,
    appointment_patient_registration_general_practice as reg_practice_at_event,
    'OP_ATTENDANCE' as visit_occurrence_type
    from {{ ref('stg_sus_op_appointment') }}
),
apc_demo as (
    select primarykey_id
    , sk_patient_id
    , spell_admission_date as code_date
    , spell_patient_identity_spell_age as age_at_event
    , spell_patient_identity_gender as gender_at_event
    , spell_patient_identity_ethnic_category as ethnicity_at_event
    , spell_patient_residence_derived_postcode_district as postcode_district_at_event
    , spell_patient_residence_derived_lsoa_11 as lsoa_11_at_event
    , spell_patient_residence_derived_local_authority_district as lad_at_event
    , spell_patient_residence_derived_index_of_multiple_deprivation_decile as imd_at_event
    , spell_patient_registration_general_practice as reg_practice_at_event
    , 'APC_SPELL' as visit_occurrence_type
    from {{ ref('stg_sus_apc_spell') }}
),
ae_demo as (
    select primarykey_id
    , sk_patient_id
    , attendance_arrival_date as code_date
    , patient_age_at_arrival as age_at_event
    , patient_stated_gender as gender_at_event
    , patient_ethnic_category as ethnicity_at_event
    , patient_usual_address_postcode_district as postcode_district_at_event
    , patient_usual_address_lsoa_11 as lsoa_11_at_event
    , patient_usual_address_local_authority_district as lad_at_event
    , patient_usual_address_index_of_multiple_deprivation_decile as imd_at_event
    , patient_gp_registration_general_practice as reg_practice_at_event
    , 'AE_ATTENDANCE' as visit_occurrence_type
    from {{ ref('stg_sus_ecds_emergency_care') }} ),

wl_demo as (
    select submission_id as primarykey_id -- replace with referral? using submission as is numeric and linkable back
    , sk_patient_id
    , referral_request_received_date as code_date
    , age_at_referral_to_treatment_period_start_date as age_at_event
    , person_stated_gender_code as gender_at_event
    , ethnic_category as ethnicity_at_event
    , null::varchar as postcode_district_at_event
    , null::varchar as lsoa_11_at_event
    , null::varchar as lad_at_event
    , null::varchar as imd_at_event
    , practice_code as reg_practice_at_event
    , 'WL_START' as visit_occurrence_type
    from {{ ref('stg_wl_openpathways_data') }} 
    where week_ending_date is not null 
    qualify row_number() over (partition by referral_identifier order by week_ending_date) = 1 -- take the first record per referral_identifier
),

all_demographics as (
    select * from op_demo
    union 
    select * from apc_demo
    union 
    select * from ae_demo
    union 
    select * from wl_demo
)

select primarykey_id as visit_occurrence_id
    , visit_occurrence_type
    , sk_patient_id
    , code_date
    , age_at_event
    , gender_at_event
    , gen.gender as gender_desc_at_event
    , ethnicity_at_event
    , eth.ethnicity_desc as ethnicity_desc_at_event
    , postcode_district_at_event
    , lsoa_11_at_event
    , lad_at_event
    , imd_at_event
    , reg_practice_at_event
from all_demographics as ad
left join ethnicity_codes as eth
    on ad.ethnicity_at_event = eth.bk_ethnicity_code
left join gender_codes as gen
    on ad.gender_at_event = gen.gender_code
where ad.sk_patient_id is not null and ad.sk_patient_id <> '1' and primarykey_id is not null
