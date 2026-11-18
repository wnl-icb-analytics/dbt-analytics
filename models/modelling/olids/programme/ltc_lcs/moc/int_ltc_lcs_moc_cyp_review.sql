-- CYP asthma review evidence, one row per person with an AR or DTR event.
with observations as (
    {{ get_ltc_lcs_observations('b7780aad-7443-896e-ebf6-1e3c3800226d,fd4dd2f3-c33a-e7d2-3556-448b9e23662b') }}
),
events as (
    select
        person_id,
        case when valueset_id = 'b7780aad-7443-896e-ebf6-1e3c3800226d' then 'AR' else 'DTR' end as event_code,
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
-- EMIS [CYPAST_HR_DTR]: the latest DTR or the latest AR counts only with another AR/DTR
-- dated from 3 months before it to before the day prior.
linked as (
    select
        l.person_id,
        max(case when e.event_date >= dateadd(month, -3, l.desktop_review_date)
            and e.event_date < dateadd(day, -1, l.desktop_review_date) then 1 else 0 end) = 1 as dtr_linked,
        max(case when e.event_date >= dateadd(month, -3, l.annual_review_date)
            and e.event_date < dateadd(day, -1, l.annual_review_date) then 1 else 0 end) = 1 as ar_linked
    from latest_dates l
    inner join events e on l.person_id = e.person_id
    group by l.person_id
),
flags as (
    select
        l.*,
        coalesce(l.desktop_review_date > dateadd(year, -1, current_date()) and k.dtr_linked, false) as dtr_qualifies,
        coalesce(l.annual_review_date > dateadd(year, -1, current_date()) and k.ar_linked, false) as ar_qualifies
    from latest_dates l
    left join linked k on l.person_id = k.person_id
)
select
    person_id,
    annual_review_date,
    desktop_review_date,
    coalesce(annual_review_date > dateadd(year, -1, current_date()), false) as annual_review_completed,
    (dtr_qualifies or ar_qualifies) as desktop_review_completed,
    greatest_ignore_nulls(
        case when dtr_qualifies then desktop_review_date end,
        case when ar_qualifies then annual_review_date end
    ) as desktop_review_completed_date
from flags
