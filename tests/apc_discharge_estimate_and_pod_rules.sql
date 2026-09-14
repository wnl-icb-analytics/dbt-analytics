-- Aggregate-only regression evidence for the agreed APC discharge estimate,
-- duration and emergency point-of-delivery rules. Returning categories rather
-- than spell keys keeps failing-test output free of record-level data.
with episode_ends as (
    select
        primarykey_id
        , max(end_date)::date as latest_submitted_episode_end_date
    from {{ ref('stg_sus_apc_spell_episodes') }}
    where end_date is not null
    group by primarykey_id
),

expected as (
    select
        s.primarykey_id
        , s.spell_discharge_date as submitted_end_date
        , case
            when s.spell_open_spell_indicator = 0
                and s.spell_discharge_date is null
                then coalesce(
                    e.latest_submitted_episode_end_date,
                    dateadd(day, s.spell_discharge_length_of_hospital_stay, s.spell_admission_date)::date
                )
            end as estimated_end_date
        , case
            when e.latest_submitted_episode_end_date is not null
                and estimated_end_date is not null
                then 'EPISODE_END_DATE'
            when estimated_end_date is not null
                then 'ADMISSION_PLUS_GROUPER_LOS'
            end as estimate_method
        , case
            when s.spell_discharge_date is not null
                then datediff(day, s.spell_admission_date, s.spell_discharge_date)
            when s.spell_open_spell_indicator = 1
                then datediff(day, s.spell_admission_date, current_date)
            end as expected_duration_to_date
        , s.spell_admission_method
    from {{ ref('stg_sus_apc_spell') }} as s
    left join episode_ends as e on s.primarykey_id = e.primarykey_id
),

failures as (
    select 'submitted_end_date_changed' as check_name, count(*) as failure_count
    from expected as e
    inner join {{ ref('int_sus_apc_encounter') }} as a
        on e.primarykey_id = a.visit_occurrence_id
    where e.submitted_end_date is distinct from a.end_date

    union all

    select 'estimated_end_date_or_method', count(*)
    from expected as e
    inner join {{ ref('int_sus_apc_encounter') }} as a
        on e.primarykey_id = a.visit_occurrence_id
    where e.estimated_end_date is distinct from a.estimated_discharge_date
        or (e.estimated_end_date is not null) is distinct from a.has_estimated_discharge_date
        or e.estimate_method is distinct from a.estimated_discharge_date_method

    union all

    select 'duration_to_date', count(*)
    from expected as e
    inner join {{ ref('int_sus_apc_encounter') }} as a
        on e.primarykey_id = a.visit_occurrence_id
    where e.expected_duration_to_date is distinct from a.duration_to_date

    union all

    select 'emergency_pod_without_discharge', count(*)
    from expected as e
    inner join {{ ref('int_sus_apc_encounter') }} as a
        on e.primarykey_id = a.visit_occurrence_id
    where e.spell_admission_method in ('21', '22', '23', '24', '25', '28', '2A', '2B', '2C', '2D')
        and e.submitted_end_date is null
        and a.pod is not null
)

select check_name, failure_count
from failures
where failure_count > 0
