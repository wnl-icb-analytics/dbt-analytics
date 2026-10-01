select
    row_id,
    {{ consistent_sk_patient_id_format('pseudo_nhs_number')}} as sk_patient_id, 
    primary_care_provider as practice_code,
    to_date(primary_care_provider_business_effective_from_date) as event_from_date,
    to_date(primary_care_provider_business_effective_to_date) as event_to_date,
    reason_for_removal as registered_reason_for_removal,
    der_ccg_of_registration,
    der_current_ccg_of_registration,
    der_icb_of_registration,
    der_current_icb_of_registration
from {{ ref('raw_pds_pds_patient_care_practice') }}
