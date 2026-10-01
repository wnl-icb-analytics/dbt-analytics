-- LSOA 2021 -> ward 2025 -> LAD 2025 from REFERENCE.GEO.LSOA_WARD_EW.
-- Replaces DATA_LAKE__NCL.ANALYST_MANAGED.LSOA21_WARD25_LAD25 (identical
-- coverage and ward/LAD assignment, verified 35,672/35,672). resident_flag
-- is derived from LAD codes; the derivation reproduces the legacy flag
-- exactly across all rows.
select
    lsoa_code as lsoa21_cd,
    lsoa_name as lsoa21_nm,
    ward_code as wd25_cd,
    ward_name as wd25_nm,
    lad_code as lad25_cd,
    lad_name as lad25_nm,
    case
        -- Barnet, Camden, Enfield, Haringey, Islington
        when lad_code in ('E09000003', 'E09000007', 'E09000010', 'E09000014', 'E09000019')
            then 'NCL'
        when lad_code like 'E09%' then 'Other London'
        else 'Outside London'
    end as resident_flag
from {{ ref('raw_reference_geo_lsoa_ward_ew') }}
