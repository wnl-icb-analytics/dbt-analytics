{{
    config(
        description="Raw layer: WNL registered neighbourhood (practice -> neighbourhood), NCL + NWL (tagged by ICB_GROUP).. 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.WNL_GP_PRACTICE_NEIGHBOURHOOD \ndbt: source(''reference_analyst_managed'', ''WNL_GP_PRACTICE_NEIGHBOURHOOD'') \nColumns:\n  PRACTICE_CODE -> practice_code\n  PRACTICE_NAME -> practice_name\n  NEIGHBOURHOOD_NAME -> neighbourhood_name\n  NEIGHBOURHOOD_CODE -> neighbourhood_code\n  ICB_GROUP -> icb_group\n  DATE_UPLOADED -> date_uploaded"
    )
}}
select
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_NAME" as practice_name,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name,
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "ICB_GROUP" as icb_group,
    "DATE_UPLOADED" as date_uploaded
from {{ source('reference_analyst_managed', 'WNL_GP_PRACTICE_NEIGHBOURHOOD') }}
