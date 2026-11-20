{{ config(cluster_by=['measure_code', 'action_at']) }}

-- Measure rules follow the NHS England e-RS dashboard metadata, version 1.4 (June 2024).
with actions as (
    select
        action_id,
        ubrn_id,
        action_code,
        action_at,
        previous_ubrn_id,
        provider_organisation_code
    from {{ ref('fct_ers_referral_action') }}
    where action_code in (
        '1407', '1408', '1409', '1412', '1420', '1422', '1424',
        '1430', '1533', '1608', '1770', '1810'
    )
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

-- Latest accept or auto-accept for each initial referral; auto-accept wins a same-time tie.
referrals_accepted as (
    select 'referrals_accepted' as measure_code, action_id
    from actions
    where action_code in ('1420', '1533')
        and previous_ubrn_id is null
    qualify row_number() over (
        partition by ubrn_id
        order by action_at desc, iff(action_code = '1533', 0, 1), action_id desc
    ) = 1
),

-- First booking, deferral or triage request for each request and provider.
referrals_received as (
    select 'referrals_received_by_provider' as measure_code, action_id
    from actions
    where action_code in ('1412', '1430', '1608')
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
    a.action_id,
    a.ubrn_id,
    a.ubrn,
    a.sk_patient_id,
    a.action_at,
    a.financial_year,
    a.financial_month,
    a.financial_month_name,
    a.week_end_date,
    a.action_code,
    a.action_name,
    a.action_reason_code,
    a.action_reason_name,
    a.referring_organisation_code,
    a.referring_organisation_name,
    a.referrer_commissioner_code,
    a.referrer_commissioner_name,
    a.provider_organisation_code,
    a.provider_organisation_name,
    a.service_id,
    a.service_name,
    a.specialty_code,
    a.specialty_name,
    a.service_specialty_code,
    a.service_specialty_name,
    a.clinic_type_code,
    a.clinic_type_name,
    a.priority_code,
    a.priority_name,
    a.registered_practice_code,
    a.registered_practice_name,
    a.registration_local_authority_code,
    a.registration_local_authority_name,
    a.residence_local_authority_code,
    a.residence_local_authority_name,
    m.advice_request_action_id,
    m.advice_requested_at,
    m.advice_response_working_hours,
    floor(m.advice_response_working_hours / 24) as advice_response_working_days
from measure_actions as m
inner join {{ ref('fct_ers_referral_action') }} as a
    on m.action_id = a.action_id
