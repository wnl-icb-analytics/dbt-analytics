{{
    config(
        description="Raw layer (Curated primary care organisation reference - practices, PCNs, neighbourhoods and memberships). 1:1 passthrough with cleaned column names. \nSource: REFERENCE.PRIMARY_CARE.PRACTICE_SUCCESSION \ndbt: source(''reference_primary_care'', ''PRACTICE_SUCCESSION'') \nColumns:\n  PREDECESSOR_CODE -> predecessor_code\n  PREDECESSOR_NAME -> predecessor_name\n  SUCCESSOR_CODE -> successor_code\n  SUCCESSOR_NAME -> successor_name\n  EFFECTIVE_DATE -> effective_date\n  RELATIONSHIP -> relationship\n  SUB_ICB_CODE -> sub_icb_code\n  SOURCE_SYSTEM -> source_system\n  NOTE -> note\n  CREATED_BY -> created_by\n  CREATED_AT -> created_at"
    )
}}
select
    "PREDECESSOR_CODE" as predecessor_code,
    "PREDECESSOR_NAME" as predecessor_name,
    "SUCCESSOR_CODE" as successor_code,
    "SUCCESSOR_NAME" as successor_name,
    "EFFECTIVE_DATE" as effective_date,
    "RELATIONSHIP" as relationship,
    "SUB_ICB_CODE" as sub_icb_code,
    "SOURCE_SYSTEM" as source_system,
    "NOTE" as note,
    "CREATED_BY" as created_by,
    "CREATED_AT" as created_at
from {{ source('reference_primary_care', 'PRACTICE_SUCCESSION') }}
