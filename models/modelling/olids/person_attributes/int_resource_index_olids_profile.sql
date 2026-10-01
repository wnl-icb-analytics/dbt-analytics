/*
Current OLIDS clinical profile for the NCL resource-index deep-dive.

Fragmented OLIDS person_ids are aggregated to sk_patient_id before joining
reporting facts.
*/

{{ config(materialized = 'table') }}

with bridge as (
    select person_id, sk_patient_id
    from {{ ref('dim_person_pseudo') }}
),

ccms as (
    select
        b.sk_patient_id,
        max(c.cambridge_comorbidity_score) as cambridge_comorbidity_score
    from {{ ref('dim_person_ccms') }} as c
    inner join bridge as b
        on c.person_id = b.person_id
    group by 1
),

ltc as (
    select
        b.sk_patient_id,
        count(distinct l.condition_code)                      as ltc_count,
        count(distinct iff(l.is_qof, l.condition_code, null)) as qof_ltc_count
    from {{ ref('fct_person_ltc_summary') }} as l
    inner join bridge as b
        on l.person_id = b.person_id
    group by 1
),

frailty as (
    select
        b.sk_patient_id,
        max(case f.latest_frailty_severity
            when 'Severe' then 3
            when 'Moderate' then 2
            when 'Mild' then 1
        end) as frailty_rank
    from {{ ref('fct_person_frailty_register') }} as f
    inner join bridge as b
        on f.person_id = b.person_id
    group by 1
),

sk_universe as (
    select distinct sk_patient_id from bridge
)

select
    u.sk_patient_id,
    ccms.cambridge_comorbidity_score,
    coalesce(ltc.ltc_count, 0)     as ltc_count,
    coalesce(ltc.qof_ltc_count, 0) as qof_ltc_count,
    case frailty.frailty_rank
        when 3 then 'Severe'
        when 2 then 'Moderate'
        when 1 then 'Mild'
    end as frailty_severity
from sk_universe as u
left join ccms
    on u.sk_patient_id = ccms.sk_patient_id
left join ltc
    on u.sk_patient_id = ltc.sk_patient_id
left join frailty
    on u.sk_patient_id = frailty.sk_patient_id
