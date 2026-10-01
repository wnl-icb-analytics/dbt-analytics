select
    sk_admission_method_id,
    bk_admission_method_code,
    admission_method_name,
    admission_method_group,
    admission_method_method_full_name,
    date_created,
    date_updated
from {{ ref('raw_dictionary_ip_admissionmethods') }}