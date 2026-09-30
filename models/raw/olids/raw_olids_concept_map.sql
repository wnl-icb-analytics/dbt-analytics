{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.CONCEPT_MAP \ndbt: source(''olids'', ''CONCEPT_MAP'') \nColumns:\n  SOURCE_CONCEPT_ID -> source_concept_id\n  SOURCE_CODE -> source_code\n  SOURCE_DISPLAY -> source_display\n  SOURCE_SYSTEM -> source_system\n  TARGET_CONCEPT_ID -> target_concept_id\n  TARGET_CODE -> target_code\n  TARGET_DISPLAY -> target_display\n  TARGET_SYSTEM -> target_system\n  IS_PRIMARY -> is_primary\n  EQUIVALENCE -> equivalence\n  EQUIVALENCE_RANK -> equivalence_rank\n  LAST_UPDATED_DATE -> last_updated_date"
    )
}}
select
    "SOURCE_CONCEPT_ID" as source_concept_id,
    "SOURCE_CODE" as source_code,
    "SOURCE_DISPLAY" as source_display,
    "SOURCE_SYSTEM" as source_system,
    "TARGET_CONCEPT_ID" as target_concept_id,
    "TARGET_CODE" as target_code,
    "TARGET_DISPLAY" as target_display,
    "TARGET_SYSTEM" as target_system,
    "IS_PRIMARY" as is_primary,
    "EQUIVALENCE" as equivalence,
    "EQUIVALENCE_RANK" as equivalence_rank,
    "LAST_UPDATED_DATE" as last_updated_date
from {{ source('olids', 'CONCEPT_MAP') }}
