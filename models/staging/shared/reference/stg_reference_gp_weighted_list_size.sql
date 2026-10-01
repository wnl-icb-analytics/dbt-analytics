select
    site,
    financial_quarter_date,
    practice_code,
    practice_name,
    gms_pms_flag,
    commissioner_code,
    commissioner_name,
    practice_list_size,
    practice_normalised_weighted_list_size,
    report_execution_datetime
    -- Excluded (redundant):
    -- pct
from {{ ref('raw_reference_gp_weighted_list_size') }}
