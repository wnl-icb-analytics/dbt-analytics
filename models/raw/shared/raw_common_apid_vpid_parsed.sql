{{
    config(
        description="Raw layer: dm+d actual-product to virtual-product mapping, parsed from AMP_RAW. Route: UNCONFIRMED, derived in-schema from the AMP_RAW landing blob. Known issue: rows are duplicated 2x (328,522 rows, 164,261 distinct pairs, identical to COMMON.APID_VPID). No consumer today.. 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.COMMON.APID_VPID_PARSED \ndbt: source(''common'', ''APID_VPID_PARSED'') \nColumns:\n  APID -> apid\n  VPID -> vpid"
    )
}}
select
    "APID" as apid,
    "VPID" as vpid
from {{ source('common', 'APID_VPID_PARSED') }}
