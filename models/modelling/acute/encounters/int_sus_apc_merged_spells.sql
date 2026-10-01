with spells as (
    -- same population/filters as fct_person_sus_apc_recent, clipped to the 12-month window
    -- (mirrors the in_year_duration clipping: start floored at window_start, end left as-is)
    select
        sk_patient_id,
        greatest(start_date, dateadd(month, -12, current_date())) as clip_start,
        end_date                                                  as clip_end
    from {{ ref('int_sus_apc_imputed_spells') }}
    where (start_date between dateadd(month, -12, current_date()) and current_date()
        or end_date   between dateadd(month, -12, current_date()) and current_date())
      and sk_patient_id is not null and sk_patient_id != '1'
      and spell_admission_method not in ('2C', '82', '31') -- birth of a baby
      and start_date is not null
      and end_date   is not null
),

clipped as (
    select * from spells
    where clip_end >= clip_start         -- keep same-day spells (scored 0.5 below); drop only negative-length
),

flagged as (
    -- a spell starts a NEW island only if it begins after the furthest end seen so far.
    -- the running max end over prior rows also captures fully-nested spells correctly.
    select
        sk_patient_id,
        clip_start,
        clip_end,
        case
            when max(clip_end) over (
                    partition by sk_patient_id
                    order by clip_start, clip_end
                    rows between unbounded preceding and 1 preceding
                 ) >= clip_start
            then 0 else 1
        end as is_new_island
    from clipped
),

islands as (
    select
        sk_patient_id,
        clip_start,
        clip_end,
        sum(is_new_island) over (
            partition by sk_patient_id
            order by clip_start, clip_end
            rows between unbounded preceding and current row
        ) as island_id
    from flagged
),

merged as (
    -- collapse each set of overlapping/nested spells into one maximal interval
    select
        sk_patient_id,
        island_id,
        min(clip_start) as island_start,
        max(clip_end)   as island_end
    from islands
    group by sk_patient_id, island_id
)

-- non-overlapping length of stay = sum of merged island lengths.
-- a same-day island (start = end, e.g. a day-case admission) counts as 0.5 rather than 0.
select
    sk_patient_id,
    sum(
        case
            when island_start = island_end then 0.5
            else datediff(day, island_start, island_end)
        end
    ) as apc_los_12mo
from merged
group by sk_patient_id
