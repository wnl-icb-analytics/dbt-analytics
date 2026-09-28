{{ config(materialized='view', tags=['ers', 'large_periodic']) }}

select
    p.appointment_id,
    a.action_id
from {{ ref('fct_ers_referral_action') }} as a
inner join {{ ref('fct_ers_appointment') }} as p
    on a.ubrn_id = p.ubrn_id
    and equal_null(a.service_id, p.service_id)
    and a.appointment_at = p.appointment_at
where a.appointment_at is not null
