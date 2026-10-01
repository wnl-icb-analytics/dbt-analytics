{{
    config(
        description="Raw layer (NHS Payments to General Practice - annual practice-level file (national). One row per practice per financial year; registered and Carr-Hill weighted list sizes plus total payments. FY 2015/16 onwards (earlier years lack the weighted column). Loaded by NHS_PAYMENTS_GP.INGEST_NHS_PAYMENTS_GP().). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.NHS_PAYMENTS_GP.PRACTICE_PAYMENTS \ndbt: source(''nhs_payments_gp'', ''PRACTICE_PAYMENTS'') \nColumns:\n  FINANCIAL_YEAR -> financial_year\n  PRACTICE_CODE -> practice_code\n  PRACTICE_NAME -> practice_name\n  COMMISSIONER_CODE -> commissioner_code\n  COMMISSIONER_NAME -> commissioner_name\n  AVERAGE_REGISTERED_PATIENTS -> average_registered_patients\n  AVERAGE_WEIGHTED_PATIENTS -> average_weighted_patients\n  TOTAL_NHS_PAYMENTS -> total_nhs_payments\n  SOURCE_URL -> source_url\n  _LOADED_AT -> loaded_at"
    )
}}
select
    "FINANCIAL_YEAR" as financial_year,
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_NAME" as practice_name,
    "COMMISSIONER_CODE" as commissioner_code,
    "COMMISSIONER_NAME" as commissioner_name,
    "AVERAGE_REGISTERED_PATIENTS" as average_registered_patients,
    "AVERAGE_WEIGHTED_PATIENTS" as average_weighted_patients,
    "TOTAL_NHS_PAYMENTS" as total_nhs_payments,
    "SOURCE_URL" as source_url,
    "_LOADED_AT" as loaded_at
from {{ source('nhs_payments_gp', 'PRACTICE_PAYMENTS') }}
