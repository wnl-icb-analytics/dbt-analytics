{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.LTC_LCS_ORIGINAL_CODES \ndbt: source(''reference_terminology'', ''LTC_LCS_ORIGINAL_CODES'') \nColumns:\n  ORIGINAL_CODE_ID -> original_code_id\n  VALUESET_ID -> valueset_id\n  ORIGINAL_CODE -> original_code\n  DISPLAY_NAME -> display_name\n  CODE_SYSTEM -> code_system\n  INCLUDE_CHILDREN -> include_children\n  IS_REFSET -> is_refset\n  TRANSLATED_TO_SNOMED_CODE -> translated_to_snomed_code\n  TRANSLATED_TO_DISPLAY -> translated_to_display\n  TRANSLATION_EQUIVALENCE -> translation_equivalence"
    )
}}
select
    "ORIGINAL_CODE_ID" as original_code_id,
    "VALUESET_ID" as valueset_id,
    "ORIGINAL_CODE" as original_code,
    "DISPLAY_NAME" as display_name,
    "CODE_SYSTEM" as code_system,
    "INCLUDE_CHILDREN" as include_children,
    "IS_REFSET" as is_refset,
    "TRANSLATED_TO_SNOMED_CODE" as translated_to_snomed_code,
    "TRANSLATED_TO_DISPLAY" as translated_to_display,
    "TRANSLATION_EQUIVALENCE" as translation_equivalence
from {{ source('reference_terminology', 'LTC_LCS_ORIGINAL_CODES') }}
