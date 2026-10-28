select *
from {{ ref('raw_ukhfd_data_dictionary_care_professional_type') }}
where is_latest = 1