/*
WNL registration exposure by patient-month.

One row per patient with WNL registration in a month covered by the SLAM cost
index. This replaces current-registration flags as the population base, so
deceased and deducted patients remain in the months where they had exposure.

PDS does not close registration episodes at death, so exposure is truncated at
the death month. The death month itself counts.

PDS also leaves episodes open when a patient is deducted without registering
elsewhere in England (embarkation, removal): the episode keeps a null
event_to_date and the deduction lands in stg_pds_reason_for_removal instead.
Untreated, those patients stay "registered" forever (~700k at WNL practices,
inflating the monthly list ~13% over the published count). Exposure is
therefore also truncated at the earliest removal that starts on or after the
episode start; the removal month itself counts. A later episode (re-registration)
is unaffected.
*/
{{ config(materialized = 'table') }}

with bounds as (
    select
        min(activity_month) as start_month,
        max(activity_month) as end_month
    from {{ ref('int_cost_index_slam_activity_monthly') }}
),

months as (
    select dateadd(month, s.seq, b.start_month) as reg_month
    from (
        select row_number() over (order by null) - 1 as seq
        from table(generator(rowcount => 120))
    ) as s
    cross join bounds as b
    where dateadd(month, s.seq, b.start_month) <= b.end_month
),

wnl_episodes_raw as (
    select
        e.sk_patient_id,
        e.event_from_date,
        coalesce(e.event_to_date, '9999-12-31'::date) as event_to_date,
        e.practice_code,
        e.row_id
    from {{ ref('stg_pds_patient_care_practice') }} as e
    join {{ ref('dim_practice') }} as d
        on e.practice_code = d.practice_code
        and d.is_wnl_practice
),

-- earliest deduction starting on/after each episode's start caps that episode
-- (last_day so the removal month still counts)
wnl_episodes as (
    select
        e.sk_patient_id,
        e.event_from_date,
        least(
            e.event_to_date,
            coalesce(last_day(min(r.event_from_date)), e.event_to_date)
        ) as event_to_date,
        e.practice_code,
        e.row_id
    from wnl_episodes_raw as e
    left join {{ ref('stg_pds_reason_for_removal') }} as r
        on r.sk_patient_id = e.sk_patient_id
        and r.event_from_date >= e.event_from_date
        and r.event_from_date <= e.event_to_date
    group by e.sk_patient_id, e.event_from_date, e.event_to_date,
        e.practice_code, e.row_id
),

person as (
    select sk_patient_id, date_of_death
    from {{ ref('int_person_pds_latest_record') }}
),

-- one row per patient-month covered by an episode; latest-starting episode
-- wins the month's practice attribution
person_months as (
    select
        e.sk_patient_id,
        m.reg_month,
        e.practice_code
    from wnl_episodes as e
    join months as m
        on e.event_from_date <= last_day(m.reg_month)
        and e.event_to_date >= m.reg_month
    qualify row_number() over (
        partition by e.sk_patient_id, m.reg_month
        order by e.event_from_date desc, e.row_id desc
    ) = 1
),

alive_months as (
    select
        pm.sk_patient_id,
        pm.reg_month,
        pm.practice_code,
        p.date_of_death
    from person_months as pm
    left join person as p
        on p.sk_patient_id = pm.sk_patient_id
    -- exposure runs to the death month inclusive
    where p.date_of_death is null
       or date_trunc('month', p.date_of_death) >= pm.reg_month
)

select
    a.sk_patient_id,
    a.reg_month as activity_month,
    a.practice_code,
    a.date_of_death,
    coalesce(a.date_of_death between a.reg_month and last_day(a.reg_month), false) as died_in_month
from alive_months as a
