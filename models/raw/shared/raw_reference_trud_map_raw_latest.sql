{{
    config(
        description="Raw layer (SNOMED CT cross-map and terminology reference data. Sourced by NHS TRUD.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.TERMINOLOGY.TRUD_MAP_RAW__LATEST \ndbt: source(''reference_trud_terminology'', ''TRUD_MAP_RAW__LATEST'') \nColumns:\n  ID -> id\n  EFFECTIVE_TIME -> effective_time\n  ACTIVE -> active\n  MODULE_ID -> module_id\n  REFSET_ID -> refset_id\n  REFERENCED_COMPONENT_ID -> referenced_component_id\n  MAP_GROUP -> map_group\n  MAP_PRIORITY -> map_priority\n  MAP_RULE -> map_rule\n  MAP_ADVICE -> map_advice\n  MAP_TARGET -> map_target\n  CORRELATION_ID -> correlation_id\n  MAP_BLOCK -> map_block\n  MAP_SYSTEM -> map_system\n  MAP_VERSION -> map_version\n  RELEASE_ID -> release_id\n  RELEASE_DATE -> release_date\n  _LOADED_AT -> loaded_at"
    )
}}
select
    "ID" as id,
    "EFFECTIVE_TIME" as effective_time,
    "ACTIVE" as active,
    "MODULE_ID" as module_id,
    "REFSET_ID" as refset_id,
    "REFERENCED_COMPONENT_ID" as referenced_component_id,
    "MAP_GROUP" as map_group,
    "MAP_PRIORITY" as map_priority,
    "MAP_RULE" as map_rule,
    "MAP_ADVICE" as map_advice,
    "MAP_TARGET" as map_target,
    "CORRELATION_ID" as correlation_id,
    "MAP_BLOCK" as map_block,
    "MAP_SYSTEM" as map_system,
    "MAP_VERSION" as map_version,
    "RELEASE_ID" as release_id,
    "RELEASE_DATE" as release_date,
    "_LOADED_AT" as loaded_at
from {{ source('reference_trud_terminology', 'TRUD_MAP_RAW__LATEST') }}
