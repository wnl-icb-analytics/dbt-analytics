select *
from {{ ref('raw_ukhfd_data_dictionary_method_of_communication_for_end_of_cancer_faster_diagnosis_pathway') }}
where is_latest = 1