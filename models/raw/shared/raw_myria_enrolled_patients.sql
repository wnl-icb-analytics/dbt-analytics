{{
    config(
        description="Raw layer (Project Myria (BEVC) canonical inputs - Doccla enrolled patients and the propensity-matched evaluation population. One row per patient per file; FILE_DATE identifies the upload. Loaded by MYRIA.LOAD_NEW_FILES().). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.MYRIA.ENROLLED_PATIENTS \ndbt: source(''myria'', ''ENROLLED_PATIENTS'') \nColumns:\n  HX_ID -> hx_id\n  ACTIVATED_DATE -> activated_date\n  ENROLLED_DATE -> enrolled_date\n  ONBOARDED_DATE -> onboarded_date\n  DISCHARGED_DATE -> discharged_date\n  FILE_DATE -> file_date\n  SOURCE_FILE -> source_file\n  LOADED_AT -> loaded_at"
    )
}}
select
    "HX_ID" as hx_id,
    "ACTIVATED_DATE" as activated_date,
    "ENROLLED_DATE" as enrolled_date,
    "ONBOARDED_DATE" as onboarded_date,
    "DISCHARGED_DATE" as discharged_date,
    "FILE_DATE" as file_date,
    "SOURCE_FILE" as source_file,
    "LOADED_AT" as loaded_at
from {{ source('myria', 'ENROLLED_PATIENTS') }}
