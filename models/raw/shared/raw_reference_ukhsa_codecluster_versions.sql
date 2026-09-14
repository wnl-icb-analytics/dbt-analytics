{{
    config(
        description="Raw layer (Reference terminology data including SNOMED, BNF, and other code sets). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.TERMINOLOGY.UKHSA_CODECLUSTER_VERSIONS \ndbt: source(''reference_terminology'', ''UKHSA_CODECLUSTER_VERSIONS'') \nColumns:\n  PROGRAMME -> programme\n  SPEC_VERSION -> spec_version\n  RELEASE_DATE -> release_date\n  CODING_SCHEME -> coding_scheme\n  LIBRARY -> library\n  CLUSTER_ID -> cluster_id\n  CLUSTER_DESCRIPTION -> cluster_description\n  SNOMED_CODE -> snomed_code\n  SNOMED_DESCRIPTION -> snomed_description\n  SOURCE_FILE -> source_file\n  LOADED_AT -> loaded_at"
    )
}}
select
    "PROGRAMME" as programme,
    "SPEC_VERSION" as spec_version,
    "RELEASE_DATE" as release_date,
    "CODING_SCHEME" as coding_scheme,
    "LIBRARY" as library,
    "CLUSTER_ID" as cluster_id,
    "CLUSTER_DESCRIPTION" as cluster_description,
    "SNOMED_CODE" as snomed_code,
    "SNOMED_DESCRIPTION" as snomed_description,
    "SOURCE_FILE" as source_file,
    "LOADED_AT" as loaded_at
from {{ source('reference_terminology', 'UKHSA_CODECLUSTER_VERSIONS') }}
