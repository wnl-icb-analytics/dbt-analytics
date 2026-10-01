{{
    config(
        description="Raw layer: WNL residence neighbourhood (LSOA 2021 -> neighbourhood), all 13 NCL + NWL boroughs. Name-only (no neighbourhood code).. 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.WNLNEIGHBOURHOODS \ndbt: source(''reference_analyst_managed'', ''WNLNEIGHBOURHOODS'') \nColumns:\n  LSOA21CD -> lsoa21_cd\n  LSOA21NM -> lsoa21_nm\n  LACODE -> lacode\n  LANAME -> laname\n  IMDRANK -> imdrank\n  IMD25DECIL -> imd25_decil\n  IMD19DECIL -> imd19_decil\n  IMDDEPRIIN -> imddepriin\n  INT -> int\n  INTBOROUGH -> intborough"
    )
}}
select
    "LSOA21CD" as lsoa21_cd,
    "LSOA21NM" as lsoa21_nm,
    "LACODE" as lacode,
    "LANAME" as laname,
    "IMDRANK" as imdrank,
    "IMD25DECIL" as imd25_decil,
    "IMD19DECIL" as imd19_decil,
    "IMDDEPRIIN" as imddepriin,
    "INT" as int,
    "INTBOROUGH" as intborough
from {{ source('reference_analyst_managed', 'WNLNEIGHBOURHOODS') }}
