select {{ consistent_sk_patient_id_format('patient_id')}} as patient_id
    , area_code
    , practice_code
    , cohort_event
    , is_active
    , event_written_at
from {{ ref('raw_c_ltcs_cltcs_inclusion_cohort_archive') }}
