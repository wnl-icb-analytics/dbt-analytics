select
    {{ consistent_sk_patient_id_format('sk_patient_id')}} as sk_patient_id,
    -- normalise feed literals ('National Data Opt-out', 'Opt-In') to the
    -- established contract (NATIONAL_DATA_OPT_OUT, OPT_IN) downstream filters on
    upper(translate(preference_type, ' -', '__')) as preference_type,
    upper(replace(preference_status, '-', '_')) as preference_status,
    effective_from,
    effective_to,
    is_latest,
    lds_record_id,
    lds_business_id,
    lds_is_deleted
from {{ ref('raw_olids_national_data_opt_out') }}
where coalesce(lds_is_deleted, false) = false
