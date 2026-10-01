select
    sk_ethnicity_id,
    bk_ethnicity_code,
    ethnicity_hes_code,
    ethnicity_code_type,
    ethnicity_combined_code,
    ethnicity_desc,
    ethnicity_desc2,
    ethnicity_desc_read,
    date_start,
    date_end,
    date_last_update
from {{ ref('raw_dictionary_dbo_ethnicity') }}