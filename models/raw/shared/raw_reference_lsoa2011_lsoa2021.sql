{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules. TURNAROUND_TIMES_RAW is not in this source; use int_tat_turnaround_times.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.LSOA2011_LSOA2021 \ndbt: source(''reference_analyst_managed'', ''LSOA2011_LSOA2021'') \nColumns:\n  LSOA11CD -> lsoa11_cd\n  LSOA11NM -> lsoa11_nm\n  LSOA21CD -> lsoa21_cd\n  LSOA21NM -> lsoa21_nm\n  CHGIND -> chgind\n  LAD22CD -> lad22_cd\n  LAD22NM -> lad22_nm\n  LAD22NMW -> lad22_nmw\n  OBJECTID -> objectid"
    )
}}
select
    "LSOA11CD" as lsoa11_cd,
    "LSOA11NM" as lsoa11_nm,
    "LSOA21CD" as lsoa21_cd,
    "LSOA21NM" as lsoa21_nm,
    "CHGIND" as chgind,
    "LAD22CD" as lad22_cd,
    "LAD22NM" as lad22_nm,
    "LAD22NMW" as lad22_nmw,
    "OBJECTID" as objectid
from {{ source('reference_analyst_managed', 'LSOA2011_LSOA2021') }}
