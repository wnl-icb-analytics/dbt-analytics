select
    cluster_id,
    cluster_description,
    snomed_code::varchar as snomed_code,
    snomed_code_description,
    pcd_refset_id,
    service_and_ruleset,
    snapshot_date,
    release_version,
    source_file
from {{ ref('raw_reference_terminology_pcd_refset_latest') }}
