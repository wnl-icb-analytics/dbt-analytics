-- open cases stay on the patient list, at their status log area, until closed, removed or rejected
select patient_id
    , area_code
    , action
    , action_date
from {{ ref('cltcs_status_log_most_recent') }}
where action is null
    or action not in ('Case closed', 'Removed from shortlist', 'Reject')
