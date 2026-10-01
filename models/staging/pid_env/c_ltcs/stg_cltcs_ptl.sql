select
    cast(sk_patient_id as varchar) as sk_patient_id,
    pcn_code,
    record_source,
    demographics_date_of_mdt,
    summaries_desktop_review_outcome,
    mdt_impact_score,
    mdt_clinician_value,
    mdt_suitable_case_study,
    metadata_file_path,
    metadata_file_row_number,
    metadata_record_ingestion_timestamp,
    metadata_file_content_key,
    metadata_file_last_modified
from {{ ref('raw_cltcs_emis_ptl') }}
where sk_patient_id is not null
