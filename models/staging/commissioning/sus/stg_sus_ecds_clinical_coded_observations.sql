select
    primarykey_id as visit_occurrence_id
    , coded_observations_id as source_sequence
    , rownumber_id as source_row_id
    , code as observation_code
    , is_code_approved
    , value as observation_value
    , ucum_unit_of_measurement as ucum_unit_code
    , timestamp as observed_at
    , dmic_import_log_id
from {{ ref('raw_sus_ecds_clinical_coded_observations') }}
