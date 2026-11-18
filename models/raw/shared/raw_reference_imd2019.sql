{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules. TURNAROUND_TIMES_RAW is not in this source; use int_tat_turnaround_times.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.IMD2019 \ndbt: source(''reference_analyst_managed'', ''IMD2019'') \nColumns:\n  LSOACODE -> lsoacode\n  IMDDECILE -> imddecile"
    )
}}
select
    "LSOACODE" as lsoacode,
    "IMDDECILE" as imddecile
from {{ source('reference_analyst_managed', 'IMD2019') }}
