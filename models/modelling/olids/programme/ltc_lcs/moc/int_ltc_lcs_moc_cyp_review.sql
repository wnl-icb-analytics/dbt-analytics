-- CYP asthma review evidence, one row per person with an AR or DTR event.
with observations as (
    {{ get_ltc_lcs_observations('229356ea-2e6c-41fc-9970-4ff96efb09b9,588fcdb7-58e7-400c-b772-c3d48017372d') }}
),
events as (
    select
        person_id,
        case when valueset_id = '229356ea-2e6c-41fc-9970-4ff96efb09b9' then 'AR' else 'DTR' end as event_code,
        clinical_effective_date::date as event_date
    from observations
    where clinical_effective_date::date <= current_date()
    group by person_id, event_code, event_date
),
latest_dates as (
    select
        person_id,
        max(case when event_code = 'AR' then event_date end) as annual_review_date,
        max(case when event_code = 'DTR' then event_date end) as desktop_review_date
    from events
    group by person_id
),
linked_annual_reviews as (
    select l.person_id
    from latest_dates l
    inner join events e on l.person_id = e.person_id
        and e.event_date >= dateadd(month, -3, l.annual_review_date)
        and e.event_date < dateadd(day, -1, l.annual_review_date)
    group by l.person_id
)
select
    l.person_id,
    l.annual_review_date,
    l.desktop_review_date,
    coalesce(l.annual_review_date > dateadd(year, -1, current_date()), false) as annual_review_completed,
    (
        coalesce(l.desktop_review_date > dateadd(year, -1, current_date()), false)
        or (coalesce(l.annual_review_date > dateadd(year, -1, current_date()), false) and linked.person_id is not null)
    ) as desktop_review_completed,
    greatest_ignore_nulls(
        case when l.desktop_review_date > dateadd(year, -1, current_date()) then l.desktop_review_date end,
        case when l.annual_review_date > dateadd(year, -1, current_date()) and linked.person_id is not null
            then l.annual_review_date end
    ) as desktop_review_completed_date
from latest_dates l
left join linked_annual_reviews linked on l.person_id = linked.person_id
