select *
from {{ ref('raw_ukhfd_data_dictionary_cancer_care_setting_treatment') }}
where is_latest = 1