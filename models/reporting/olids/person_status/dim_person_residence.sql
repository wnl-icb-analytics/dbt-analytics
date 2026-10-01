with base as (select
    person_id,
    sk_patient_id,
    uprn_hash
from {{ ref('dim_person_demographics') }}
where uprn_hash is not null
),

frailty as (select
    person_id,
    1 as frail_flag
from {{ ref('fct_person_frailty_register') }}
),

frailty_table as (
    select
        base.person_id,
        base.sk_patient_id,
        base.uprn_hash,
        frailty.frail_flag
    from base
    left join frailty on base.person_id = frailty.person_id
),

household_size as (
    select
        uprn_hash,
        count (*) as household_registered_count,
        sum (frail_flag) as frail_household_count
    from frailty_table
    group by uprn_hash
)

select bs.person_id
    , bs.sk_patient_id
    , bs.uprn_hash
    , hs.household_registered_count
    , hs.frail_household_count
    , case when hs.frail_household_count =  hs.household_registered_count then 1 else 0 end as frail_household_flag
from base bs
left join household_size hs on bs.uprn_hash = hs.uprn_hash
where bs.uprn_hash is not null