select *
from {{ ref('raw_ukhfd_data_dictionary_cancer_faster_diagnosis_pathway_exclusion_reason') }}
where is_latest = 1