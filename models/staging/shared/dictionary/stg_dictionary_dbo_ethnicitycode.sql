select
    sk_ethnicity_id,
    ethnicity_code_type,
    upper(trim(ethnic_category_code)) as ethnic_category_code,
    ethnic_group_code,
    ic_code,
    pds_ethnic_category_code,
    read_code,
    sde_code,
    trim(description) as description,
    priority,
    snomed::varchar as snomed_code
from {{ ref('raw_dictionary_dbo_ethnicitycode') }}
