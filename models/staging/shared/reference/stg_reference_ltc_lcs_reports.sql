select
    report_id,
    report_name,
    search_name,
    folder_path,
    xml_file_name,
    parsed_at
from {{ ref('raw_reference_ltc_lcs_reports') }}
