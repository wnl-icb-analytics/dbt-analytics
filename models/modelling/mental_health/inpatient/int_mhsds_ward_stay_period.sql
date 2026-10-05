with selected as (
    select *
    from {{ ref('stg_mhsds_ward_stay_history') }}
    qualify row_number() over (
        partition by uniq_submission_id, uniq_ward_stay_id
        order by effective_from desc nulls last, row_number desc nulls last, mhs502_uniq_id desc
    ) = 1
)
, intervals as (
    select
        s.*
        , {{ dbt_utils.generate_surrogate_key(['uniq_submission_id','uniq_ward_stay_id']) }} as ward_stay_period_id
        , greatest(start_date_ward_stay, reporting_period_start_date) as period_stay_start_date
        , least(coalesce(end_date_ward_stay, dateadd(day, 1, reporting_period_end_date)),
            dateadd(day, 1, reporting_period_end_date)) as period_stay_end_date_exclusive
        , start_date_ward_stay is null or coalesce(end_date_ward_stay < start_date_ward_stay, false)
            as has_invalid_stay_dates
        , iff(has_invalid_stay_dates, null,
            greatest(0, datediff(day, period_stay_start_date, period_stay_end_date_exclusive)))
            as recorded_ward_stay_midnights
    from selected as s
)
select
    i.*
    -- Mark both sides of an overlap, including stays attributed to different wards.
    , coalesce(person_id is not null and (period_stay_start_date < max(period_stay_end_date_exclusive) over (
        partition by uniq_submission_id, person_id
        order by period_stay_start_date, ward_stay_period_id
        rows between unbounded preceding and 1 preceding
    ) or period_stay_end_date_exclusive > lead(period_stay_start_date) over (
        partition by uniq_submission_id, person_id
        order by period_stay_start_date, ward_stay_period_id
    )), false) as has_overlapping_person_stay_in_submission
from intervals as i
