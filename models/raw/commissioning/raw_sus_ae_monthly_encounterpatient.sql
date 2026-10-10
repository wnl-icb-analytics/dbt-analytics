{{
    config(
        description="Raw layer (SUS Monthly emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterPatient \ndbt: source(''sus_ae_monthly'', ''EncounterPatient'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  SK_GenderID -> sk_gender_id\n  SK_EthnicityID -> sk_ethnicity_id\n  SK_PostcodeID -> sk_postcode_id\n  SK_PracticeID -> sk_practice_id\n  Date_of_Birth -> date_of_birth\n  Age -> age\n  SK_Org_PracticeID -> sk_org_practice_id\n  WardCode -> ward_code\n  LSOACode -> lsoa_code"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "SK_GenderID" as sk_gender_id,
    "SK_EthnicityID" as sk_ethnicity_id,
    "SK_PostcodeID" as sk_postcode_id,
    "SK_PracticeID" as sk_practice_id,
    "Date_of_Birth" as date_of_birth,
    "Age" as age,
    "SK_Org_PracticeID" as sk_org_practice_id,
    "WardCode" as ward_code,
    "LSOACode" as lsoa_code
from {{ source('sus_ae_monthly', 'EncounterPatient') }}
