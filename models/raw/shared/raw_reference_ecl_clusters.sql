{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.ECL_CLUSTERS \ndbt: source(''reference_terminology'', ''ECL_CLUSTERS'') \nColumns:\n  CLUSTER_ID -> cluster_id\n  ECL_EXPRESSION -> ecl_expression\n  DESCRIPTION -> description\n  CREATED_AT -> created_at\n  UPDATED_AT -> updated_at\n  CREATED_BY -> created_by\n  UPDATED_BY -> updated_by\n  CLUSTER_TYPE -> cluster_type\n  CLUSTER_UID -> cluster_uid"
    )
}}
select
    "CLUSTER_ID" as cluster_id,
    "ECL_EXPRESSION" as ecl_expression,
    "DESCRIPTION" as description,
    "CREATED_AT" as created_at,
    "UPDATED_AT" as updated_at,
    "CREATED_BY" as created_by,
    "UPDATED_BY" as updated_by,
    "CLUSTER_TYPE" as cluster_type,
    "CLUSTER_UID" as cluster_uid
from {{ source('reference_terminology', 'ECL_CLUSTERS') }}
