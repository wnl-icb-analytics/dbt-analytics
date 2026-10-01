--Field criteria
/*
gender_code (Most recent non-'unknown' gender code)
age_at_event (Most recent non-null record (should it be max?))
ethnicity_code (Most recent non-'unknown' ethnicity)
lsoa_21 (Most recent non-'unknown' lsoa)
practice_code (Most recent non-'unknown' practice_code)
*/
with base as (
    select
        sk_patient_id,
        gender_at_event,
        gender_desc_at_event,
        age_at_event,
        ethnicity_at_event,
        ethnicity_desc_at_event,
        lsoa_11_at_event,
        reg_practice_at_event,
        code_date,
        visit_occurrence_id
    from {{ref('int_encounter_demographics')}}
    where code_date::date <= current_date()
),

--ctes to reduce field values into 1 per patient
gender_event as (
    select 
        sk_patient_id, 
        coalesce(gender_at_event, 'X') as gender_code,
        coalesce(gender_desc_at_event, 'Not Known') as gender_desc,
        code_date::date as gender_event_date,
        
        --Rank rows for this field to identify which to use
        row_number() over (
            partition by sk_patient_id 
            order by
                --Prioritise stated gender values over 'Unknown' or invalid codes
                case
                    when gender_at_event in ('1','2','9') then 1
                    when gender_at_event = '0' then 2
                    else 3
                end,
                --Prefer more recent records
                code_date desc,
                --Occurrence ID only if a tie-breaker is needed
                visit_occurrence_id desc
        ) as gender_field_rank
        
    from base
    qualify gender_field_rank = 1
),

int_age_gap as (
    select
        sk_patient_id,
        visit_occurrence_id,
        age_at_event,
        code_date,
        max(code_date) over (partition by sk_patient_id, age_at_event) as latest_code_date,
        datediff(year, code_date, latest_code_date) as gap_to_latest
        
    from base
    where code_date is not null
    and age_at_event <= 120
),

age_event as (
    select 
        sk_patient_id, 
        age_at_event,
        --Estimate dob
        --Criteria: Assume birthday in the month of the earliest activity at their current age
        -- excluding activity more than a year earlier than the latest activity at their current age
        dateadd(year, -1*age_at_event, 
            date_trunc('month', min(code_date::date) over (
                partition by sk_patient_id, age_at_event
            )) 
        ) as date_of_birth,
        code_date::date as dob_event_date,
        
        --Rank rows for this field to identify which to use
        row_number() over (
            partition by sk_patient_id 
            order by
                --Prioritise non-null values
                case
                    when age_at_event is not null then 1
                    else 2
                end,
                --Prefer more recent records
                code_date desc,
                --Occurrence ID only if a tie-breaker is needed
                visit_occurrence_id desc
        ) as age_field_rank
        
    from int_age_gap
    
    --Filter out records where the current age is used more than a year ago
    where gap_to_latest = 0
    
    qualify age_field_rank = 1
),

ethnicity_event as (
    select 
        sk_patient_id, 
        ethnicity_at_event as ethnicity_code,
        ethnicity_desc_at_event,
        code_date::date as ethnicity_event_date,
        
        --Rank rows for this field to identify which to use
        row_number() over (
            partition by sk_patient_id 
            order by
                --Prioritise ethnicity values that are stated and not null
                case
                    when ethnicity_at_event not in ('Z', '99') and ethnicity_at_event is not null then 1
                    when ethnicity_at_event in ('Z', '99') then 2
                    else 3
                end,
                --Prefer more recent records
                code_date desc,
                --Occurrence ID only if a tie-breaker is needed
                visit_occurrence_id desc
        ) as ethnicity_field_rank
        
    from base
    qualify ethnicity_field_rank = 1
),

lsoa_event as (
    select
        sk_patient_id,
        lsoa_11_at_event as lsoa_11,
        code_date::date as lsoa_event_date,

        --Rank rows for this field to identify which to use
        row_number() over (
            partition by sk_patient_id
            order by
                --Prioritise non-null 2011 LSOA values
                case
                    when lsoa_11_at_event is not null then 1
                    else 2
                end,
                --Prefer more recent records
                code_date desc,
                --Occurrence ID only if a tie-breaker is needed
                visit_occurrence_id desc
        ) as lsoa_field_rank

    from base
    qualify lsoa_field_rank = 1
),

registered_event as (
    select 
        sk_patient_id, 
        reg_practice_at_event as practice_code,
        code_date::date as registered_event_date,
        
        --Rank rows for this field to identify which to use
        row_number() over (
            partition by sk_patient_id 
            order by
                --Prioritise practice code values that are not null
                case
                    when reg_practice_at_event is not null then 1
                    else 2
                end,
                --Prefer more recent records
                code_date desc,
                --Occurrence ID only if a tie-breaker is needed
                visit_occurrence_id desc
        ) as registered_field_rank
        
    from base
    qualify registered_field_rank = 1
)

select
    'sus' as dataset_source,
    base.sk_patient_id,
    gen.gender_code,
    gen.gender_event_date,
    age.date_of_birth,
    age.dob_event_date,
    eth.ethnicity_code,
    eth.ethnicity_event_date,
    map_lsoa.lsoa21_cd as lsoa21_code,
    res.lsoa_event_date,
    reg.practice_code,
    reg.registered_event_date

from (select distinct sk_patient_id from base) as base

--Join to get key field values
left join gender_event gen
on base.sk_patient_id = gen.sk_patient_id

left join age_event age
on base.sk_patient_id = age.sk_patient_id

left join ethnicity_event eth
on base.sk_patient_id = eth.sk_patient_id

left join lsoa_event res
on base.sk_patient_id = res.sk_patient_id

--Join to map LSOA 2011 codes to LSOA 2021 (done after ranking in the CTE)
left join {{ref('stg_reference_lsoa2011_lsoa2021')}} map_lsoa
on res.lsoa_11 = map_lsoa.lsoa11_cd

left join registered_event reg
on base.sk_patient_id = reg.sk_patient_id