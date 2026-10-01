{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.derived \ndbt: source(''sus_ecds'', ''derived'') \nColumns:\n  PRIMARYKEY_ID -> primarykey_id\n  CqcCareHomeCode -> cqc_care_home_code\n  dmicICBCommissioner -> dmic_icb_commissioner\n  dmicSubICBCommissioner -> dmic_sub_icb_commissioner\n  dmicICBRegistrationSubmitted -> dmic_icb_registration_submitted\n  dmicICBResidenceSubmitted -> dmic_icb_residence_submitted\n  dmicSubICBRegistrationSubmitted -> dmic_sub_icb_registration_submitted\n  dmicSubICBResidenceSubmitted -> dmic_sub_icb_residence_submitted\n  dmicCommissionerDerivationReason -> dmic_commissioner_derivation_reason\n  dmicImportLogId -> dmic_import_log_id\n  dmicLSOA2021 -> dmic_lsoa2021\n  dmicElectoralWardCode -> dmic_electoral_ward_code"
    )
}}
select
    "PRIMARYKEY_ID" as primarykey_id,
    "CqcCareHomeCode" as cqc_care_home_code,
    "dmicICBCommissioner" as dmic_icb_commissioner,
    "dmicSubICBCommissioner" as dmic_sub_icb_commissioner,
    "dmicICBRegistrationSubmitted" as dmic_icb_registration_submitted,
    "dmicICBResidenceSubmitted" as dmic_icb_residence_submitted,
    "dmicSubICBRegistrationSubmitted" as dmic_sub_icb_registration_submitted,
    "dmicSubICBResidenceSubmitted" as dmic_sub_icb_residence_submitted,
    "dmicCommissionerDerivationReason" as dmic_commissioner_derivation_reason,
    "dmicImportLogId" as dmic_import_log_id,
    "dmicLSOA2021" as dmic_lsoa2021,
    "dmicElectoralWardCode" as dmic_electoral_ward_code
from {{ source('sus_ecds', 'derived') }}
