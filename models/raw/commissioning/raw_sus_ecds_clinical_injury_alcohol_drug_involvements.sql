{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.clinical.injury.alcohol_drug_involvements \ndbt: source(''sus_ecds'', ''clinical.injury.alcohol_drug_involvements'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  ALCOHOL_DRUG_INVOLVEMENTS_ID -> alcohol_drug_involvements_id\n  code -> code\n  is_code_approved -> is_code_approved\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "ALCOHOL_DRUG_INVOLVEMENTS_ID" as alcohol_drug_involvements_id,
    "code" as code,
    "is_code_approved" as is_code_approved,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_ecds', 'clinical.injury.alcohol_drug_involvements') }}
