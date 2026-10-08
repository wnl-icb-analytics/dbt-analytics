-- raw_reference_csds_lookup.sql

select *
from {{ source('reference_analyst_managed', 'CSDS_LOOKUP') }}