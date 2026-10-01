{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.clinical.diagnoses.snomed.max_core_diagnoses \ndbt: source(''sus_ecds'', ''clinical.diagnoses.snomed.max_core_diagnoses'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  SNOMED_ID -> snomed_id\n  MAX_CORE_DIAGNOSES_ID -> max_core_diagnoses_id\n  code -> code\n  equivalent_ae_code -> equivalent_ae_code\n  is_injury_related -> is_injury_related\n  is_aec_related -> is_aec_related\n  is_allergy_related -> is_allergy_related\n  is_applicable_to_females -> is_applicable_to_females\n  is_applicable_to_males -> is_applicable_to_males\n  is_notifiable_disease -> is_notifiable_disease\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "SNOMED_ID" as snomed_id,
    "MAX_CORE_DIAGNOSES_ID" as max_core_diagnoses_id,
    "code" as code,
    "equivalent_ae_code" as equivalent_ae_code,
    "is_injury_related" as is_injury_related,
    "is_aec_related" as is_aec_related,
    "is_allergy_related" as is_allergy_related,
    "is_applicable_to_females" as is_applicable_to_females,
    "is_applicable_to_males" as is_applicable_to_males,
    "is_notifiable_disease" as is_notifiable_disease,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_ecds', 'clinical.diagnoses.snomed.max_core_diagnoses') }}
