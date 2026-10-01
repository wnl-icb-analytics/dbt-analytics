{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.ECL_CLUSTER_VERSIONS \ndbt: source(''reference_terminology'', ''ECL_CLUSTER_VERSIONS'') \nColumns:\n  VERSION_ID -> version_id\n  VERSION_HASH -> version_hash\n  CONTENT_HASH -> content_hash\n  ECL_EXPRESSION_HASH -> ecl_expression_hash\n  CLUSTER_UID -> cluster_uid\n  CLUSTER_ID -> cluster_id\n  VERSION_NUMBER -> version_number\n  ECL_EXPRESSION -> ecl_expression\n  DESCRIPTION -> description\n  CLUSTER_TYPE -> cluster_type\n  CHANGE_TYPE -> change_type\n  SOURCE_VERSION_ID -> source_version_id\n  CREATED_AT -> created_at\n  CREATED_BY -> created_by\n  COMMENT -> comment"
    )
}}
select
    "VERSION_ID" as version_id,
    "VERSION_HASH" as version_hash,
    "CONTENT_HASH" as content_hash,
    "ECL_EXPRESSION_HASH" as ecl_expression_hash,
    "CLUSTER_UID" as cluster_uid,
    "CLUSTER_ID" as cluster_id,
    "VERSION_NUMBER" as version_number,
    "ECL_EXPRESSION" as ecl_expression,
    "DESCRIPTION" as description,
    "CLUSTER_TYPE" as cluster_type,
    "CHANGE_TYPE" as change_type,
    "SOURCE_VERSION_ID" as source_version_id,
    "CREATED_AT" as created_at,
    "CREATED_BY" as created_by,
    "COMMENT" as comment
from {{ source('reference_terminology', 'ECL_CLUSTER_VERSIONS') }}
