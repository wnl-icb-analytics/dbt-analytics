-- Clinical-item and standard drill-down keys identify the same row.
select
    'clinical_item_drill_down_key_mismatch' as failure_reason
    , source_table
    , clinical_record_type
    , count(*) as failures
from {{ ref('fct_mhsds_clinical_record') }}
where source_record_id is distinct from clinical_record_id
group by source_table, clinical_record_type
