-- Date-only source events must remain distinguishable from recorded midnight.
with event_times as (
    select 'referral' as source_entity, source_record_id, referral_received_date as event_date, referral_received_time as event_time,
        referral_received_at as event_at, referral_received_time_precision as precision
    from {{ ref('fct_csds_referral') }}
    union all
    select 'contact', source_record_id, care_contact_date, care_contact_time, care_contact_at, care_contact_time_precision
    from {{ ref('fct_csds_care_contact') }}
)
select source_entity, source_record_id, event_date, event_time, event_at, precision
from event_times
where (event_date is null and (event_at is not null or precision is not null))
    or (event_date is not null and (event_at is null or event_at::date <> event_date))
    or (event_date is not null and event_time is null
        and (precision is distinct from 'date' or event_at::time <> '00:00:00'::time))
    or (event_date is not null and event_time is not null
        and (precision is distinct from 'timestamp' or event_at::time <> event_time))
