{{
    config(
        description="Raw layer (SUS Monthly emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterLegalStatus \ndbt: source(''sus_ae_monthly'', ''EncounterLegalStatus'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  Sequence_Number -> sequence_number\n  SK_MentalHealthLegalStatus_ID -> sk_mental_health_legal_status_id\n  Assignment_Period_Start_Date -> assignment_period_start_date\n  Assignment_Period_Start_Time -> assignment_period_start_time\n  Expiry_Date -> expiry_date\n  Expiry_Time -> expiry_time\n  Is_Approved -> is_approved"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "Sequence_Number" as sequence_number,
    "SK_MentalHealthLegalStatus_ID" as sk_mental_health_legal_status_id,
    "Assignment_Period_Start_Date" as assignment_period_start_date,
    "Assignment_Period_Start_Time" as assignment_period_start_time,
    "Expiry_Date" as expiry_date,
    "Expiry_Time" as expiry_time,
    "Is_Approved" as is_approved
from {{ source('sus_ae_monthly', 'EncounterLegalStatus') }}
