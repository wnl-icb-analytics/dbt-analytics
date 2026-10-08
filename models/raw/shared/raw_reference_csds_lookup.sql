-- raw_reference_csds_lookup.sql
{{
    config(
        description="Raw layer for csds_lookup, as used in CSDS Contact"
    )
}}
select *
from {{ source('reference_analyst_managed', 'CSDS_LOOKUP') }}