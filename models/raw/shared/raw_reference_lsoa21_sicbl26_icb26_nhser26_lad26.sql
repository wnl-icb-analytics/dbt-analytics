{{
    config(
        description="Raw layer (Data management reference datasets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.DATA_MANAGEMENT.LSOA21_SICBL26_ICB26_NHSER26_LAD26 \ndbt: source(''reference_data_management'', ''LSOA21_SICBL26_ICB26_NHSER26_LAD26'') \nColumns:\n  LSOA21CD -> lsoa21_cd\n  LSOA21NM -> lsoa21_nm\n  SICBL26CD -> sicbl26_cd\n  SICBL26CDH -> sicbl26_cdh\n  SICBL26NM -> sicbl26_nm\n  ICB26CD -> icb26_cd\n  ICB26CDH -> icb26_cdh\n  ICB26NM -> icb26_nm\n  NHSER26CD -> nhser26_cd\n  NHSER26CDH -> nhser26_cdh\n  NHSER26NM -> nhser26_nm\n  LAD26CD -> lad26_cd\n  LAD26NM -> lad26_nm\n  OBJECTID -> objectid"
    )
}}
select
    "LSOA21CD" as lsoa21_cd,
    "LSOA21NM" as lsoa21_nm,
    "SICBL26CD" as sicbl26_cd,
    "SICBL26CDH" as sicbl26_cdh,
    "SICBL26NM" as sicbl26_nm,
    "ICB26CD" as icb26_cd,
    "ICB26CDH" as icb26_cdh,
    "ICB26NM" as icb26_nm,
    "NHSER26CD" as nhser26_cd,
    "NHSER26CDH" as nhser26_cdh,
    "NHSER26NM" as nhser26_nm,
    "LAD26CD" as lad26_cd,
    "LAD26NM" as lad26_nm,
    "OBJECTID" as objectid
from {{ source('reference_data_management', 'LSOA21_SICBL26_ICB26_NHSER26_LAD26') }}
