-- Source-level retained receipt/extraction ages for October 2025 to July 2026.
-- One APC discharge, OP slot or ECDS arrival per source record; selected e-RS
-- actions and OLIDS slots. Record-weighted ages do not measure first delivery.
with activity as (
    select source_dataset, event_date, source_received_at
    from {{ ref('int_sus_healthcare_event') }}
    where (source_dataset = 'SUS_APC' and event_type = 'discharge')
       or (source_dataset = 'SUS_OP' and event_type = 'appointment_slot')
       or (source_dataset = 'ECDS' and event_type = 'emergency_care_arrival')
    union all
    select source_dataset, event_date, source_received_at
    from {{ ref('int_ers_healthcare_event') }}
    where source_record_type = 'referral_action'
    union all
    select source_dataset, event_date, source_received_at
    from {{ ref('int_olids_healthcare_event') }}
    where event_type = 'appointment_slot'
), lags as (
    select *, datediff(day, event_date, source_received_at) as lag_days
    from activity where event_date between '2025-10-01' and '2026-07-31'
)
select source_dataset,
    round(100.0 * count_if(source_received_at is null) / count(*)) as missing_delivery_pct,
    round(100.0 * count_if(lag_days < 0) / count(*)) as negative_lag_pct,
    round(percentile_cont(.5) within group
        (order by iff(lag_days >= 0, lag_days, null))) as retained_receipt_p50_days,
    round(percentile_cont(.9) within group
        (order by iff(lag_days >= 0, lag_days, null))) as retained_receipt_p90_days,
    round(percentile_cont(.95) within group
        (order by iff(lag_days >= 0, lag_days, null))) as retained_receipt_p95_days
from lags group by source_dataset having count(*) >= 100;
