{{
    config(
        description="Raw layer (OLIDS stable layer - cleaned and filtered patient records). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.OLIDS.CONCEPT \ndbt: source(''olids'', ''CONCEPT'') \nColumns:\n  CONCEPT_ID -> concept_id\n  CODE -> code\n  DISPLAY -> display\n  SYSTEM -> system\n  PRESENT_IN_TERMINOLOGY_SERVER -> present_in_terminology_server\n  IS_MAPPED -> is_mapped\n  USE_COUNT -> use_count"
    )
}}
select
    "CONCEPT_ID" as concept_id,
    "CODE" as code,
    "DISPLAY" as display,
    "SYSTEM" as system,
    "PRESENT_IN_TERMINOLOGY_SERVER" as present_in_terminology_server,
    "IS_MAPPED" as is_mapped,
    "USE_COUNT" as use_count
from {{ source('olids', 'CONCEPT') }}
