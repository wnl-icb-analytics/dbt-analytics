{{
    config(
        materialized = 'table',
        tags=['ers']
        )
    }}
    select
    version,
    org_id_referrer,
    uniq_submission_id,
    ebsx00_id,
    file_type,
    rp_start_date,
    rp_end_date,
    unique_month_id,
    total_ebsx02,
    total_records,
    dmic_import_log_id,
    dmic_month_id,
    dmic_system_id,
    dmic_ccg_code_referrer,
    dmic_dscro,
    dmic_date_added
from {{ ref('raw_ers_pc_ebsx00header') }}
