{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules in the MODELLING environment). 1:1 passthrough with cleaned column names. \nSource: MODELLING.LOOKUP_NCL.ETHNICITY_NATIONAL_DATA_SETS \ndbt: source(''reference_lookup_ncl'', ''ETHNICITY_NATIONAL_DATA_SETS'') \nColumns:\n  DATA_SOURCE -> data_source\n  SK_PATIENTID -> sk_patientid\n  ETHNICITY_CODE -> ethnicity_code\n  ETHNICITY_DESC -> ethnicity_desc\n  ETHNICITY -> ethnicity\n  ETHNICITY_DETAIL -> ethnicity_detail\n  RECORD_DATE -> record_date\n  LOAD_DATE -> load_date"
    )
}}
select
    "DATA_SOURCE" as data_source,
    "SK_PATIENTID" as sk_patientid,
    "ETHNICITY_CODE" as ethnicity_code,
    "ETHNICITY_DESC" as ethnicity_desc,
    "ETHNICITY" as ethnicity,
    "ETHNICITY_DETAIL" as ethnicity_detail,
    "RECORD_DATE" as record_date,
    "LOAD_DATE" as load_date
from {{ source('reference_lookup_ncl', 'ETHNICITY_NATIONAL_DATA_SETS') }}
