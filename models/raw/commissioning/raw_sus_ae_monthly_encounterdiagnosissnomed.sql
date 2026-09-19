{{
    config(
        description="Raw layer (SUS Monthly emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterDiagnosisSNOMED \ndbt: source(''sus_ae_monthly'', ''EncounterDiagnosisSNOMED'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  Sequence_Number -> sequence_number\n  Code -> code\n  Qualifier -> qualifier\n  Qualifier_Is_Approved -> qualifier_is_approved\n  Coded_Clinical_Entry_Sequence_Number -> coded_clinical_entry_sequence_number\n  Is_Approved -> is_approved"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "Sequence_Number" as sequence_number,
    "Code" as code,
    "Qualifier" as qualifier,
    "Qualifier_Is_Approved" as qualifier_is_approved,
    "Coded_Clinical_Entry_Sequence_Number" as coded_clinical_entry_sequence_number,
    "Is_Approved" as is_approved
from {{ source('sus_ae_monthly', 'EncounterDiagnosisSNOMED') }}
