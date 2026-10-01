select
    row_id,
    {{ consistent_sk_patient_id_format('pseudo_nhs_number')}} as sk_patient_id,
    reason_for_removal,
    reason_for_removal_business_effective_from_date as event_from_date,
    reason_for_removal_business_effective_to_date as event_to_date
from {{ ref('raw_pds_pds_reason_for_removal') }}
