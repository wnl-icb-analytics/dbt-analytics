{{
    config(
        description="Raw layer: Master POD group mapping. One row per (point_of_delivery_code, local_point_of_delivery_code, local_point_of_delivery_description) combination, assigned to one of the 18 governed POD_GROUP_OPTION categories (pod_group_overview_master). Component fields carry real NULLs; POD_LOOKUP retained for legacy join parity.. 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.POD_GROUP_MANAGER.POD_GROUP_MAPPING \ndbt: source(''pod_group_manager'', ''POD_GROUP_MAPPING'') \nColumns:\n  POD_LOOKUP -> pod_lookup\n  POINT_OF_DELIVERY_CODE -> point_of_delivery_code\n  LOCAL_POINT_OF_DELIVERY_CODE -> local_point_of_delivery_code\n  LOCAL_POINT_OF_DELIVERY_DESCRIPTION -> local_point_of_delivery_description\n  POD_GROUP_OVERVIEW_MASTER -> pod_group_overview_master\n  OLD_POD_GROUP_OVERVIEW -> old_pod_group_overview\n  NOTES -> notes\n  INSERTION_DATE -> insertion_date\n  CREATED_BY -> created_by\n  CREATED_AT -> created_at\n  UPDATED_BY -> updated_by\n  UPDATED_AT -> updated_at"
    )
}}
select
    "POD_LOOKUP" as pod_lookup,
    "POINT_OF_DELIVERY_CODE" as point_of_delivery_code,
    "LOCAL_POINT_OF_DELIVERY_CODE" as local_point_of_delivery_code,
    "LOCAL_POINT_OF_DELIVERY_DESCRIPTION" as local_point_of_delivery_description,
    "POD_GROUP_OVERVIEW_MASTER" as pod_group_overview_master,
    "OLD_POD_GROUP_OVERVIEW" as old_pod_group_overview,
    "NOTES" as notes,
    "INSERTION_DATE" as insertion_date,
    "CREATED_BY" as created_by,
    "CREATED_AT" as created_at,
    "UPDATED_BY" as updated_by,
    "UPDATED_AT" as updated_at
from {{ source('pod_group_manager', 'POD_GROUP_MAPPING') }}
