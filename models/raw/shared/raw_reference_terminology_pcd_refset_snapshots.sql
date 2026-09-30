{{
    config(
        description="Raw layer: Primary Care Domain refset membership by release snapshot. 1:1 passthrough with cleaned column names. \nSource: REFERENCE.TERMINOLOGY.PCD_REFSET_SNAPSHOTS \ndbt: source(''reference_terminology_curated'', ''pcd_refset_snapshots'') \nColumns:\n  SNAPSHOT_DATE -> snapshot_date\n  RELEASE_VERSION -> release_version\n  SOURCE_FILE -> source_file\n  CLUSTER_ID -> cluster_id\n  CLUSTER_DESCRIPTION -> cluster_description\n  SNOMED_CODE -> snomed_code\n  SNOMED_CODE_DESCRIPTION -> snomed_code_description\n  PCD_REFSET_ID -> pcd_refset_id\n  SERVICE_AND_RULESET -> service_and_ruleset"
    )
}}
select
    "SNAPSHOT_DATE" as snapshot_date,
    "RELEASE_VERSION" as release_version,
    "SOURCE_FILE" as source_file,
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "SNOMED_CODE" as snomed_code,
    "SNOMED_CODE_DESCRIPTION" as snomed_code_description,
    "PCD_REFSET_ID" as pcd_refset_id,
    "SERVICE_AND_RULESET" as service_and_ruleset
from {{ source('reference_terminology_curated', 'pcd_refset_snapshots') }}
