select
    primarykey_id as visit_occurrence_id
    , coded_scored_assessments_id as source_sequence
    , rownumber_id as source_row_id
    , tool_type_code as assessment_tool_code
    , tool_type_is_code_approved as is_code_approved
    , person_score
    , validation_timestamp as validated_at
    , dmic_import_log_id
from {{ ref('raw_sus_ecds_clinical_coded_scored_assessments') }}
