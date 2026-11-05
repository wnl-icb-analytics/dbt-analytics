{{
    config(
        description="Raw layer: CSDS Lookup - Reference lookup table for analyst-managed reference data. Created by May Canlas1 on 2025-10-23.. 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.CSDS_LOOKUP \ndbt: source(''reference_analyst_managed'', ''CSDS_LOOKUP'') \nColumns:\n  CSDS_FIELDNAME -> csds_fieldname\n  SIMPLETABLE_FIELDNAME -> simpletable_fieldname\n  CODE -> code\n  DESCRIPTION -> description"
    )
}}
select
    "CSDS_FIELDNAME" as csds_fieldname,
    "SIMPLETABLE_FIELDNAME" as simpletable_fieldname,
    "CODE" as code,
    "DESCRIPTION" as description
from {{ source('reference_analyst_managed', 'CSDS_LOOKUP') }}
