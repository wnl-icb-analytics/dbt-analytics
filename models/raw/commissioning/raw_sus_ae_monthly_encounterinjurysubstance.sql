{{
    config(
        description="Raw layer (SUS Monthly emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_AE.EncounterInjurySubstance \ndbt: source(''sus_ae_monthly'', ''EncounterInjurySubstance'') \nColumns:\n  SK_EncounterID -> sk_encounter_id\n  Sequence_Number -> sequence_number\n  InjuryAlcoholOrDrugInvolvement_SNOMEDCT -> injury_alcohol_or_drug_involvement_snomedct\n  Is_Approved -> is_approved"
    )
}}
select
    "SK_EncounterID" as sk_encounter_id,
    "Sequence_Number" as sequence_number,
    "InjuryAlcoholOrDrugInvolvement_SNOMEDCT" as injury_alcohol_or_drug_involvement_snomedct,
    "Is_Approved" as is_approved
from {{ source('sus_ae_monthly', 'EncounterInjurySubstance') }}
