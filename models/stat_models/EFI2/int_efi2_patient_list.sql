{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['efi2'])
}}

/*
eFI2 patient list — the living-patient spine and single as-at date for the eFI2
scoring run. One row per person.

Port of lds-pipelines stg_efi2__patient_list. Substitutions vs lds (OLIDS-native) unified patient tables
and date handling simplified to single date and fixed to current date.

Gender is normalised to FEMALE / MALE / OTHER-UNKNOWN exactly as lds does.
Deceased persons whose death is on/before the end date are excluded.

Population is restricted to age >= 65 at the scoring date, matching the eFI2
paper (Age and Ageing 2025, afaf077), which developed and validated the index
only in patients aged >= 65. Scoring younger patients would apply the deficits,
weights and frailty cut-points off-label. This is a deliberate paper-alignment
deviation from lds, which scored all living adults.

date_of_birth is only accurate to the month (the day is rounded to a fixed day of
the month), so age is estimated CONSERVATIVELY: we assume the earliest possible
true birth day (the 1st of the birth month), which maximises age, so anyone who
could be 65 by the scoring date is included.
*/

with persons as (
    select
        bd.person_id,
        current_date() as end_date,
        bd.birth_date_approx as date_of_birth,
        bd.death_date_approx as date_of_death,
        dem.gender as gender_raw
    from {{ ref('dim_person_birth_death') }} bd
    left join {{ ref('dim_person_demographics') }} dem
        on bd.person_id = dem.person_id
)

select
    person_id,
    end_date,
    case
        when upper(gender_raw) in ('FEMALE', 'F')
        then 'FEMALE'
        when upper(gender_raw) in ('MALE', 'M')
        then 'MALE'
        else 'OTHER/UNKNOWN'
    end as gender,
    date_of_birth,
    date_of_death
from persons
where (date_of_death is null or date_of_death > end_date)
    -- Conservative age >= 65 (see header). date_of_birth is month-accurate only, so
    -- we assume the earliest possible birth day (1st of the birth month via
    -- date_trunc) to maximise age, and include anyone whose 65th birthday under that
    -- assumption falls on or before the scoring date. i.e. any chance of being 65 =
    -- flagged in. This correctly excludes e.g. someone born Dec 1961 today, whom a
    -- plain datediff(year, ...) would wrongly count as 65.
    and dateadd(year, 65, date_trunc('month', date_of_birth)) <= end_date
