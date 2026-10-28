select *
from {{ ref('raw_ukhfd_data_dictionary_cancer_treatment_modality') }}
where is_latest = 1