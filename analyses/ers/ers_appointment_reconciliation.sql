-- Only complete-population counts are returned.
select
    (select count(*) from {{ ref('fct_ers_referral_action') }} where appointment_at is not null) as timestamped_actions,
    (select sum(action_count) from {{ ref('fct_ers_appointment') }}) as fact_action_count,
    (select count(*) from {{ ref('rel_ers_appointment_action') }}) as related_actions,
    (select count(distinct action_id) from {{ ref('rel_ers_appointment_action') }}) as unique_related_actions,
    (select count(*) from {{ ref('fct_ers_appointment') }}) as appointments,
    (select count(distinct appointment_id) from {{ ref('fct_ers_appointment') }}) as unique_appointments
