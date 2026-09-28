select icd10_code as code, icd10_term as description
from {{ ref('raw_reference_icd10_code_term_latest') }}
