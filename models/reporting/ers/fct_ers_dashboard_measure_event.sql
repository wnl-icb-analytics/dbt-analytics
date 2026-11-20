{{ config(cluster_by=['measure_code', 'action_at']) }}

-- Rules follow the NHS England e-RS dashboard metadata v1.4 (June 2024) and the SQL published with it.
with measure_source as (
    select *
    from {{ ref('fct_ers_referral_action') }}
    where action_code in (
        '1407', '1408', '1409', '1412', '1420', '1422', '1423', '1424',
        '1430', '1533', '1608', '1770', '1810'
    )
),

actions as (
    select
        action_id,
        ubrn_id,
        action_code,
        action_at,
        week_end_date,
        previous_ubrn_id,
        provider_organisation_code
    from measure_source
),

single_action_measures as (
    select
        case action_code
            when '1422' then 'referrals'
            when '1424' then 'referrals_rejected'
            when '1409' then 'advice_outcomes'
            when '1608' then 'ras_requests'
            when '1770' then 'ras_outcomes'
            when '1430' then 'asi'
            when '1810' then 'asi_outcomes'
        end as measure_code,
        action_id
    from actions
    -- Referral measures count initial referrals only.
    where (action_code in ('1422', '1424') and previous_ubrn_id is null)
        or action_code in ('1409', '1608', '1770', '1430', '1810')
),

-- Accept or auto-accept actions at the latest accept time for each initial referral and
-- Monday-to-Sunday week, the reporting period NHS England's dashboard SQL runs over.
latest_accepts as (
    select
        ubrn_id,
        action_id,
        action_code,
        action_at,
        week_end_date
    from actions
    where action_code in ('1420', '1533')
        and previous_ubrn_id is null
    qualify action_at = max(action_at) over (partition by ubrn_id, week_end_date)
),

referral_updates as (
    select distinct ubrn_id, action_at
    from actions
    where action_code = '1423'
        and previous_ubrn_id is null
),

-- One accept per referral and week, auto-accept first. An update (1423) at the same time
-- voids the acceptance unless accept and auto-accept are both present, when the auto-accept counts.
referrals_accepted as (
    select 'referrals_accepted' as measure_code, a.action_id
    from latest_accepts as a
    left join referral_updates as u
        on a.ubrn_id = u.ubrn_id
        and a.action_at = u.action_at
    qualify row_number() over (
            partition by a.ubrn_id, a.week_end_date
            order by iff(a.action_code = '1533', 0, 1), a.action_id desc
        ) = 1
        and (
            u.ubrn_id is null
            or (
                a.action_code = '1533'
                and count_if(a.action_code = '1420') over (partition by a.ubrn_id, a.week_end_date) > 0
            )
        )
),

-- First booking, deferral or triage request for each request and provider.
referrals_received as (
    select 'referrals_received_by_provider' as measure_code, action_id
    from actions
    where action_code in ('1412', '1430', '1608')
        and provider_organisation_code is not null
    qualify row_number() over (
        partition by ubrn_id, provider_organisation_code
        order by action_at, action_id
    ) = 1
),

advice_requests as (
    select ubrn_id, action_id, action_at
    from actions
    where action_code = '1407'
    qualify row_number() over (partition by ubrn_id order by action_at, action_id) = 1
),

advice_first_responses as (
    select
        response.action_id,
        request.action_id as advice_request_action_id,
        request.action_at as advice_requested_at,
        response.action_at as advice_responded_at
    from actions as response
    inner join advice_requests as request
        on response.ubrn_id = request.ubrn_id
        and response.action_at >= request.action_at
    where response.action_code = '1408'
    qualify row_number() over (
        partition by response.ubrn_id
        order by response.action_at, response.action_id
    ) = 1
),

-- Running count of weekend and England and Wales bank-holiday dates.
calendar as (
    select
        dates.full_date,
        sum(iff(dates.is_weekend or holiday.full_date is not null, 1, 0))
            over (order by dates.full_date) as non_working_days_to_date,
        iff(dates.is_weekend or holiday.full_date is not null, 1, 0) as is_non_working_day
    from {{ ref('stg_dictionary_dbo_dates') }} as dates
    left join {{ ref('stg_dictionary_dbo_datebankholiday') }} as holiday
        on dates.full_date = holiday.full_date::date
        and holiday.in_england_and_wales
    where dates.full_date between '2010-01-01' and dateadd(year, 1, current_date)
),

-- Subtract 24 hours for each non-working date strictly between the request and response dates.
advice_response_times as (
    select
        r.action_id,
        r.advice_request_action_id,
        r.advice_requested_at,
        datediff(second, r.advice_requested_at, r.advice_responded_at) / 3600
            - 24 * greatest(
                response_day.non_working_days_to_date - response_day.is_non_working_day
                - request_day.non_working_days_to_date,
                0
            ) as advice_response_working_hours
    from advice_first_responses as r
    left join calendar as request_day
        on r.advice_requested_at::date = request_day.full_date
    left join calendar as response_day
        on r.advice_responded_at::date = response_day.full_date
),

measure_actions as (
    select
        measure_code, action_id,
        null::number as advice_request_action_id,
        null::timestamp_ntz as advice_requested_at,
        null::number(18, 6) as advice_response_working_hours
    from single_action_measures

    union all

    select measure_code, action_id, null, null, null
    from referrals_accepted

    union all

    select measure_code, action_id, null, null, null
    from referrals_received

    union all

    select 'advice_requests', action_id, null, null, null
    from advice_requests

    union all

    select
        'advice_first_responses', action_id, advice_request_action_id,
        advice_requested_at, advice_response_working_hours
    from advice_response_times
)

select
    m.measure_code,
    case m.measure_code
        when 'referrals' then 'Referrals'
        when 'referrals_accepted' then 'Referrals accepted'
        when 'referrals_rejected' then 'Referrals rejected'
        when 'advice_requests' then 'A&G requests'
        when 'advice_first_responses' then 'A&G first response'
        when 'advice_outcomes' then 'A&G outcomes'
        when 'ras_requests' then 'RAS requests'
        when 'ras_outcomes' then 'RAS outcomes'
        when 'asi' then 'ASI'
        when 'asi_outcomes' then 'ASI outcomes'
        when 'referrals_received_by_provider' then 'Referrals received by provider'
    end as measure_name,
    e.action_id,
    e.ubrn_id,
    e.ubrn,
    e.sk_patient_id,
    e.action_at,
    e.financial_year,
    e.financial_month,
    e.financial_month_name,
    e.week_end_date,
    e.action_code,
    e.action_name,
    e.action_reason_code,
    e.action_reason_name,
    -- A&G first responses take their breakdowns from the initial advice request.
    c.action_id as breakdown_action_id,
    c.referring_organisation_code,
    c.referring_organisation_name,
    c.referrer_commissioner_code,
    c.referrer_commissioner_name,
    c.provider_organisation_code,
    c.provider_organisation_name,
    c.service_id,
    c.service_name,
    iff(c.service_specialty_code is not null, c.service_specialty_code, c.specialty_code)
        as dashboard_specialty_code,
    iff(c.service_specialty_code is not null, c.service_specialty_name, c.specialty_name)
        as dashboard_specialty_name,
    c.specialty_code,
    c.specialty_name,
    c.service_specialty_code,
    c.service_specialty_name,
    c.clinic_type_code,
    c.clinic_type_name,
    c.priority_code,
    c.priority_name,
    c.registered_practice_code,
    c.registered_practice_name,
    c.registration_local_authority_code,
    c.registration_local_authority_name,
    c.residence_local_authority_code,
    c.residence_local_authority_name,
    m.advice_request_action_id,
    m.advice_requested_at,
    m.advice_response_working_hours,
    -- NHS England dashboard bands, applied to whole working hours.
    case
        when m.advice_response_working_hours is null then null
        when floor(m.advice_response_working_hours) <= 48 then '0 to 2 days'
        when floor(m.advice_response_working_hours) <= 120 then 'Over 2 to 5 days'
        when floor(m.advice_response_working_hours) <= 240 then 'Over 5 to 10 days'
        else 'Over 10 days'
    end as advice_response_time_band
from measure_actions as m
inner join measure_source as e
    on m.action_id = e.action_id
inner join measure_source as c
    on coalesce(m.advice_request_action_id, m.action_id) = c.action_id
