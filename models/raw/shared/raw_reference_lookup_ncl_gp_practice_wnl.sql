{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.GP_PRACTICE_WNL \ndbt: source(''reference_lookup_ncl'', ''GP_PRACTICE_WNL'') \nColumns:\n  SK_ORGANISATION_ID -> sk_organisation_id\n  PRACTICE_CODE -> practice_code\n  PRACTICE_NAME -> practice_name\n  PRACTICE_NAME_SHORT -> practice_name_short\n  BOROUGH -> borough\n  PCN_CODE -> pcn_code\n  PCN_NAME -> pcn_name\n  NEIGHBOURHOOD_CODE -> neighbourhood_code\n  NEIGHBOURHOOD_NAME -> neighbourhood_name"
    )
}}
select
    "SK_ORGANISATION_ID" as sk_organisation_id,
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_NAME" as practice_name,
    "PRACTICE_NAME_SHORT" as practice_name_short,
    "BOROUGH" as borough,
    "PCN_CODE" as pcn_code,
    "PCN_NAME" as pcn_name,
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name
from {{ source('reference_lookup_ncl', 'GP_PRACTICE_WNL') }}
