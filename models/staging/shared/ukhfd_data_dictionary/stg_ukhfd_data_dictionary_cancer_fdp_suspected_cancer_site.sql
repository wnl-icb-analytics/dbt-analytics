select *
from {{ ref('raw_ukhfd_data_dictionary_primary_cancer_site_for_cancer_faster_diagnosis_pathway') }}
where is_latest = 1