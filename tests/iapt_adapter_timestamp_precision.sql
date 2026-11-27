-- Adapters expose clock times only; the shared reporting views add date-only sorting anchors.
select 'healthcare_event' as adapter, count(*) as invalid_timestamp_count
from {{ ref('int_iapt_healthcare_event') }}
where event_time_precision is distinct from 'timestamp' and event_at is not null
having count(*) > 0
union all
select 'person_clinical_record' as adapter, count(*) as invalid_timestamp_count
from {{ ref('int_iapt_person_clinical_record') }}
where clinical_time_precision is distinct from 'timestamp' and clinical_record_at is not null
having count(*) > 0
