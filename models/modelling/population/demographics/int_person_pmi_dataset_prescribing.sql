-- PMI feeder: patient demographics from EPD prescribing (WNL-wide).
-- One row per sk_patient_id, each attribute reduced to its best/most-recent
-- value, emitted in the shared PMI feeder schema so int_person_pmi_combined
-- can `union by name` it alongside the pds / sus / ethnicity feeders.
-- EPD carries gender, age (-> dob estimate), residence LSOA and registered
-- practice; it has no ethnicity (emitted as NULL).
-- NOTE: built and ready but intentionally NOT yet unioned into
-- int_person_pmi_combined. PDS is ~100% complete on LSOA/practice for WNL, so
-- prescribing adds negligible spine coverage; its value is the cost models
-- (prescribing spend) and as a future recency fallback.
-- EPD is one row per prescription item; the rankings only need distinct
-- attribute values per patient and month. Ties on date take the highest value.
with base as (
    select distinct
        sk_patient_id,
        processing_period_date::date as event_date,
        patient_gender,
        try_to_number(patient_age)   as patient_age,
        patient_lsoa,
        registered_practice_code
    from {{ ref('stg_epd_pc_meds') }}
    where is_latest_submission
      and sk_patient_id is not null
      and processing_period_date::date <= current_date()
),

gender_event as (
    select
        sk_patient_id,
        case
            when upper(trim(patient_gender)) in ('1', 'M', 'MALE')   then '1'
            when upper(trim(patient_gender)) in ('2', 'F', 'FEMALE') then '2'
            else '9'
        end as gender_code,
        event_date as gender_event_date,
        row_number() over (
            partition by sk_patient_id
            order by
                case when upper(trim(patient_gender)) in ('1','2','M','F','MALE','FEMALE') then 1 else 2 end,
                event_date desc,
                patient_gender desc
        ) as gender_field_rank
    from base
    qualify gender_field_rank = 1
),

age_event as (
    select
        sk_patient_id,
        -- estimate dob: birthday in the month of the earliest activity at this age
        dateadd(year, -1 * patient_age, date_trunc('month', event_date)) as date_of_birth,
        event_date as dob_event_date,
        row_number() over (
            partition by sk_patient_id
            order by
                case when patient_age is not null then 1 else 2 end,
                event_date desc,
                patient_age desc
        ) as age_field_rank
    from base
    where patient_age is null or patient_age between 0 and 120
    qualify age_field_rank = 1
),

lsoa_event as (
    select
        b.sk_patient_id,
        -- EPD residence LSOA is 2011-vintage; map to 2021. Only emit a confirmed
        -- 2021 code (null when unmapped) so lsoa21_code never carries a 2011 code.
        map_lsoa.lsoa21_cd as lsoa_21,
        b.event_date as lsoa_event_date,
        row_number() over (
            partition by b.sk_patient_id
            order by
                case when map_lsoa.lsoa21_cd is not null then 1 else 2 end,
                b.event_date desc,
                map_lsoa.lsoa21_cd desc
        ) as lsoa_field_rank
    from base b
    left join {{ ref('stg_reference_lsoa2011_lsoa2021') }} map_lsoa
        on b.patient_lsoa = map_lsoa.lsoa11_cd
    qualify lsoa_field_rank = 1
),

registered_event as (
    select
        sk_patient_id,
        registered_practice_code as practice_code,
        event_date as registered_event_date,
        row_number() over (
            partition by sk_patient_id
            order by
                case when registered_practice_code is not null then 1 else 2 end,
                event_date desc,
                registered_practice_code desc
        ) as registered_field_rank
    from base
    qualify registered_field_rank = 1
)

select
    'epd' as dataset_source,
    base.sk_patient_id,
    gen.gender_code,
    gen.gender_event_date,
    age.date_of_birth,
    age.dob_event_date,
    null::varchar as ethnicity_code,
    null::date    as ethnicity_event_date,
    res.lsoa_21 as lsoa21_code,
    res.lsoa_event_date,
    reg.practice_code,
    reg.registered_event_date

from (select distinct sk_patient_id from base) as base

left join gender_event gen
on base.sk_patient_id = gen.sk_patient_id

left join age_event age
on base.sk_patient_id = age.sk_patient_id

left join lsoa_event res
on base.sk_patient_id = res.sk_patient_id

left join registered_event reg
on base.sk_patient_id = reg.sk_patient_id
