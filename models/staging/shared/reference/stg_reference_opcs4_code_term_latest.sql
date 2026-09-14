select opcs4_code as code, opcs4_term as description
from {{ ref('raw_reference_opcs4_code_term_latest') }}
