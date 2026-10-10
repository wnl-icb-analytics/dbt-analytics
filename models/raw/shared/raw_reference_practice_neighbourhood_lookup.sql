{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules. TURNAROUND_TIMES_RAW is not in this source; use int_tat_turnaround_times.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.PRACTICE_NEIGHBOURHOOD_LOOKUP \ndbt: source(''reference_analyst_managed'', ''PRACTICE_NEIGHBOURHOOD_LOOKUP'') \nColumns:\n  PRACTICECODE -> practicecode\n  PRACTICENAME -> practicename\n  PCNCODE -> pcncode\n  LOCALAUTHORITY -> localauthority\n  PRACTICENEIGHBOURHOOD -> practiceneighbourhood"
    )
}}
select
    "PRACTICECODE" as practicecode,
    "PRACTICENAME" as practicename,
    "PCNCODE" as pcncode,
    "LOCALAUTHORITY" as localauthority,
    "PRACTICENEIGHBOURHOOD" as practiceneighbourhood
from {{ source('reference_analyst_managed', 'PRACTICE_NEIGHBOURHOOD_LOOKUP') }}
