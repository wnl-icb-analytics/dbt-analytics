select
    sk_patientid,
    ethnicity_code,
    ethnicity_desc,
    ethnicity,
    ethnicity_detail,
    cast(record_date as date) as record_date
from {{ ref('raw_reference_lookup_ncl_ethnicity_national_data_sets') }}
