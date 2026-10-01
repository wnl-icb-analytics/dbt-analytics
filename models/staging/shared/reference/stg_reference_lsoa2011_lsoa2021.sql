{{
    config(materialized = 'table')
}}

select
    lsoa11_cd,
    lsoa11_nm,
    lsoa21_cd,
    lsoa21_nm,
    chgind,
    lad22_cd,
    lad22_nm
    -- Excluded:
    -- lad22_nmw (Welsh name - not relevant for NCL),
    -- objectid
from {{ ref('raw_reference_lsoa2011_lsoa2021') }}
qualify row_number() over (partition by lsoa11_cd order by lsoa21_cd) = 1
