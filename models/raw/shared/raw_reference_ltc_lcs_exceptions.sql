{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.LTC_LCS_EXCEPTIONS \ndbt: source(''reference_terminology'', ''LTC_LCS_EXCEPTIONS'') \nColumns:\n  EXCEPTION_ID -> exception_id\n  VALUESET_ID -> valueset_id\n  ORIGINAL_EXCLUDED_CODE -> original_excluded_code\n  ORIGINAL_EXCLUDED_DISPLAY -> original_excluded_display\n  TRANSLATED_TO_SNOMED_CODE -> translated_to_snomed_code\n  INCLUDED_IN_ECL -> included_in_ecl\n  TRANSLATION_ERROR -> translation_error"
    )
}}
select
    "EXCEPTION_ID" as exception_id,
    "VALUESET_ID" as valueset_id,
    "ORIGINAL_EXCLUDED_CODE" as original_excluded_code,
    "ORIGINAL_EXCLUDED_DISPLAY" as original_excluded_display,
    "TRANSLATED_TO_SNOMED_CODE" as translated_to_snomed_code,
    "INCLUDED_IN_ECL" as included_in_ecl,
    "TRANSLATION_ERROR" as translation_error
from {{ source('reference_terminology', 'LTC_LCS_EXCEPTIONS') }}
