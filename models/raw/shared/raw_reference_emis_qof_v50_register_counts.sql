{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules. TURNAROUND_TIMES_RAW is not in this source; use int_tat_turnaround_times.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.EMIS_QOF_V50_REGISTER_COUNTS \ndbt: source(''reference_analyst_managed'', ''EMIS_QOF_V50_REGISTER_COUNTS'') \nColumns:\n  REFERENCE_DATE -> reference_date\n  INDICATOR_CODE -> indicator_code\n  INDICATOR_DESCRIPTION -> indicator_description\n  ORGANISATION -> organisation\n  CDB -> cdb\n  POPULATION_COUNT -> population_count\n  PARENT_POPULATION -> parent_population\n  PERCENTAGE -> percentage\n  MALES -> males\n  FEMALES -> females\n  EXCLUDED -> excluded\n  STATUS -> status"
    )
}}
select
    "REFERENCE_DATE" as reference_date,
    "INDICATOR_CODE" as indicator_code,
    "INDICATOR_DESCRIPTION" as indicator_description,
    "ORGANISATION" as organisation,
    "CDB" as cdb,
    "POPULATION_COUNT" as population_count,
    "PARENT_POPULATION" as parent_population,
    "PERCENTAGE" as percentage,
    "MALES" as males,
    "FEMALES" as females,
    "EXCLUDED" as excluded,
    "STATUS" as status
from {{ source('reference_analyst_managed', 'EMIS_QOF_V50_REGISTER_COUNTS') }}
