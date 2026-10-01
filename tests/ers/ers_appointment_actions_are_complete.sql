select 'appointment_action_coverage' as failed_rule
where (
    select count(*) from {{ ref('fct_ers_referral_action') }} where appointment_at is not null
) <> (
    select count(*) from {{ ref('rel_ers_appointment_action') }}
)
