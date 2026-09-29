{{ config(tags=['ers', 'large_periodic'], cluster_by=['sk_patient_id', 'appointment_at']) }}

with appointment_history as (
    select
        ubrn_id,
        service_id,
        appointment_at,
        count(*) as action_count,
        min(action_at) as first_observed_action_at,
        max(action_at) as last_observed_action_at,
        max(action_id) as latest_action_id,
        -- Keep administrative updates from replacing the last recorded appointment action.
        max(case when action_code in (
            '1412', '1413', '1414', '1416', '1417', '1419',
            '1420', '1424', '1431', '1432', '1433', '1435', '1533'
        ) then action_id end) as latest_appointment_action_id,
        count_if(action_code = '1412') as recorded_booking_action_count,
        case when count(distinct sk_patient_id) = 1 then min(sk_patient_id) end as sk_patient_id
    from {{ ref('fct_ers_referral_action') }}
    where appointment_at is not null
    group by ubrn_id, service_id, appointment_at
)

select
    {{ dbt_utils.generate_surrogate_key([
        'h.ubrn_id', 'h.service_id', "to_char(h.appointment_at, 'YYYY-MM-DD HH24:MI:SS.FF9')"
    ]) }} as appointment_id,
    h.ubrn_id,
    a.ubrn,
    h.sk_patient_id,
    h.appointment_at,
    h.service_id,
    a.service_name,
    a.service_specialty_code,
    a.service_specialty_name,
    a.provider_organisation_code,
    a.provider_organisation_name,
    a.site_code,
    a.site_name,
    a.appointment_type_code,
    a.appointment_type_name,
    h.action_count,
    h.recorded_booking_action_count,
    h.first_observed_action_at,
    h.last_observed_action_at,
    h.latest_action_id,
    h.latest_appointment_action_id,
    status.action_at as latest_appointment_action_at,
    status.action_code as latest_appointment_action_code,
    status.action_name as latest_appointment_action_name,
    status.action_reason_code as latest_appointment_action_reason_code,
    status.action_reason_name as latest_appointment_action_reason_name
from appointment_history as h
inner join {{ ref('fct_ers_referral_action') }} as a
    on h.latest_action_id = a.action_id
left join {{ ref('fct_ers_referral_action') }} as status
    on h.latest_appointment_action_id = status.action_id
