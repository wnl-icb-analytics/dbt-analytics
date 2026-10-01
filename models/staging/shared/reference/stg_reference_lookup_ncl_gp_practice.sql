-- Active NCL practices from the curated REFERENCE database. Replaces the
-- MODELLING.LOOKUP_NCL.GP_PRACTICE view, which returns 0 rows since the
-- 2026-04 ICB merger re-parented practices in ODS. Neighbourhood codes and
-- names follow the canonical REFERENCE scheme (BAR01 / "Barnet: East"),
-- not the legacy N001 scheme.
select
    practice_code as gp_practice_code,
    practice_name,
    registered_borough_name as borough,
    pcn_code,
    pcn_name,
    neighbourhood_code,
    neighbourhood_name
from {{ ref('raw_reference_primary_care_practice_active') }}
where sub_icb_code = '93C'
