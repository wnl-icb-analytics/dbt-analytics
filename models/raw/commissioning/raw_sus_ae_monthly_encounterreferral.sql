{{
    config(
        description="Raw layer (SUS Monthly emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterReferral \ndbt: source(''sus_ae_monthly'', ''EncounterReferral'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  Sequence_Number -> sequence_number\n  Code -> code\n  Activity_Service_Request_Date -> activity_service_request_date\n  Activity_Service_Request_Time -> activity_service_request_time\n  Assessment_Date -> assessment_date\n  Assessment_Time -> assessment_time\n  Is_Approved -> is_approved"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "Sequence_Number" as sequence_number,
    "Code" as code,
    "Activity_Service_Request_Date" as activity_service_request_date,
    "Activity_Service_Request_Time" as activity_service_request_time,
    "Assessment_Date" as assessment_date,
    "Assessment_Time" as assessment_time,
    "Is_Approved" as is_approved
from {{ source('sus_ae_monthly', 'EncounterReferral') }}
