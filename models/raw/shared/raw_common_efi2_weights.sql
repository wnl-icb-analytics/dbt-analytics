{{
    config(
        description="Raw layer: eFI2 deficit weights - one row per (deficit, detail key). Route: manual upload, transcribed from the published paper (https://academic.oup.com/ageing/article/54/4/afaf077/8101467). Not automatically refreshed; 40 rows, never independently verified against the paper.. 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.COMMON.EFI2_WEIGHTS \ndbt: source(''common'', ''EFI2_WEIGHTS'') \nColumns:\n  DEFICIT -> deficit\n  DETAIL_KEY -> detail_key\n  WEIGHT -> weight"
    )
}}
select
    "DEFICIT" as deficit,
    "DETAIL_KEY" as detail_key,
    "WEIGHT" as weight
from {{ source('common', 'EFI2_WEIGHTS') }}
