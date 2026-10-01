-- Person master index (PMI): unions the per-dataset feeders (pds / sus / ethnicity)
-- by name, then resolves each attribute to its best source per patient.
-- In-area = WNL (NCL + NWL), driven off geo.resident_flag, not a single ICB code.
-- To add a feeder: emit the shared column names and union it in below.

with combined as (
    select * from {{ref('int_person_pmi_dataset_pds')}}
    union by name
    select * from {{ref('int_person_pmi_dataset_sus')}}
    union by name
    select * from {{ref('int_person_pmi_dataset_ethnicity_national_data_sets')}}
),

ids as (
    select distinct sk_patient_id
    from combined
),

gender_field as (
    select
        sk_patient_id,
        dataset_source as gender_source,
        gender_code,
        gender_event_date
        
    from combined

    where dataset_source in ('pds', 'sus')
    
    qualify row_number () over (
        partition by sk_patient_id
        order by 
            gender_code in ('1','2','9') desc,
            gender_event_date desc,
            dataset_source = 'pds' desc
    ) = 1
),

dob_field as (
    select 
        sk_patient_id,
        dataset_source as dob_source,
        date_of_birth,
        dob_event_date
        
    from combined

    where dataset_source in ('pds', 'sus')

    qualify row_number () over (
        partition by sk_patient_id
        order by 
            date_of_birth is not null desc,
            dataset_source = 'pds' desc,
            dob_event_date desc
    ) = 1
),

date_of_death_field as (
    select
        sk_patient_id,
        dataset_source as death_source,
        date_of_death,
        death_event_date
        
    from combined
    where dataset_source = 'pds'
),

ethnicity_field as (
    select 

        sk_patient_id,
        dataset_source as ethnicity_source,
        ethnicity_code,
        ethnicity_event_date
        
    from combined

    where dataset_source in ('eth', 'sus')

    qualify row_number () over (
        partition by sk_patient_id
        order by 
            ethnicity_code is not null desc,
            ethnicity_code not in ('X','Z','99') desc,
            dataset_source = 'eth' desc,
            ethnicity_event_date desc
    ) = 1
),

preferred_language_field as (
    select
        sk_patient_id,
        dataset_source as preferred_language_source,
        preferred_language_code,
        preferred_language_event_date
        
    from combined
    where dataset_source = 'pds'
),

interpreter_required_field as (

    select
        sk_patient_id,
        dataset_source as interpreter_required_source,
        interpreter_required,
        interpreter_event_date
        
    from combined
    where dataset_source = 'pds'
),

lsoa_field as (
    select 

        combined.sk_patient_id,
        combined.dataset_source as lsoa_source,
        combined.lsoa21_code,
        combined.lsoa_event_date
        
    from combined

    --Join to get current WNL LSOAs and current WNL residence population
    left join {{ref('stg_reference_lookup_ncl_lsoa_2021_ward_2025_local_authority_2025')}} geo
    on combined.lsoa21_code = geo.lsoa_2021_code
    and geo.resident_flag in ('NCL', 'NWL')

    left join {{ref('int_person_pds_population_flags')}} pop_flags
    on combined.sk_patient_id = pop_flags.sk_patient_id

    where dataset_source in ('pds', 'sus')

    qualify row_number () over (
        partition by combined.sk_patient_id
        order by 
            combined.lsoa21_code is not null desc,
            combined.lsoa_event_date desc,
            combined.dataset_source = 'pds' desc
    ) = 1
    --Extra restriction to prevent additional WNL residents from a non-pds source
    --Working on the assumption that pds contains all WNL residents
    and (
        pop_flags.flag_current_resident = TRUE or geo.lsoa_2021_code is null
    )
),
practice_code_field as (
    select 

        combined.sk_patient_id,
        combined.dataset_source as practice_source,
        combined.practice_code,
        combined.registered_event_date
        
    from combined

    --Join to get current WNL Practices and Current WNL Registered Population
    --(ODS-derived dim_practice, stable codes; replaces fragile GP_PRACTICE_WNL view)
    left join {{ref('dim_practice')}} gp_lu
    on combined.practice_code = gp_lu.practice_code
    and gp_lu.is_wnl_practice
    and gp_lu.is_active_practice

    left join {{ref('int_person_pds_population_flags')}} pop_flags
    on combined.sk_patient_id = pop_flags.sk_patient_id

    where dataset_source in ('pds', 'sus')

    qualify row_number () over (
        partition by combined.sk_patient_id
        order by 
            combined.practice_code is not null desc,
            combined.registered_event_date desc,
            combined.dataset_source = 'pds' desc
    ) = 1
    --Extra restriction to prevent additional WNL registered from a non-pds source
    --Working on the assumption that pds contains all WNL registered
    and (
        pop_flags.flag_current_registered = TRUE or gp_lu.practice_code is null
    )
)

select
    ids.sk_patient_id,
    gender_field.gender_source,
    gender_field.gender_code,
    gender_field.gender_event_date,
    dob_field.dob_source,
    dob_field.date_of_birth,
    dob_field.dob_event_date,
    date_of_death_field.death_source,
    date_of_death_field.date_of_death,
    date_of_death_field.death_event_date,
    ethnicity_field.ethnicity_source,
    ethnicity_field.ethnicity_code,
    ethnicity_field.ethnicity_event_date,
    preferred_language_field.preferred_language_source,
    preferred_language_field.preferred_language_code,
    preferred_language_field.preferred_language_event_date,
    interpreter_required_field.interpreter_required_source,
    interpreter_required_field.interpreter_required,
    interpreter_required_field.interpreter_event_date,
    lsoa_field.lsoa_source,
    lsoa_field.lsoa21_code,
    lsoa_field.lsoa_event_date,
    practice_code_field.practice_source,
    practice_code_field.practice_code,
    practice_code_field.registered_event_date,
    pop_flags.flag_current_registered,
    pop_flags.record_registered_start_date,
    pop_flags.flag_current_resident,
    pop_flags.record_residence_start_date

from ids

left join gender_field
on ids.sk_patient_id = gender_field.sk_patient_id

left join dob_field
on ids.sk_patient_id = dob_field.sk_patient_id

left join date_of_death_field
on ids.sk_patient_id = date_of_death_field.sk_patient_id

left join ethnicity_field
on ids.sk_patient_id = ethnicity_field.sk_patient_id

left join preferred_language_field
on ids.sk_patient_id = preferred_language_field.sk_patient_id

left join interpreter_required_field
on ids.sk_patient_id = interpreter_required_field.sk_patient_id

left join lsoa_field
on ids.sk_patient_id = lsoa_field.sk_patient_id

left join practice_code_field
on ids.sk_patient_id = practice_code_field.sk_patient_id

left join {{ref('int_person_pds_population_flags')}} pop_flags
on ids.sk_patient_id = pop_flags.sk_patient_id