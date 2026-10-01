-- LTC LCS: NAFLD register
-- EMIS source: 'LTC LCS: NAFLD Register v2*'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/nafld/ltc_lcs_nafld_register_v2.md)
--
-- The EMIS register spans the whole steatotic liver disease family (fatty
-- liver conditions including the alcoholic branch, plus MASLD/MASH codes) -
-- broader than fct_person_nafld_register, which stays on the clinically
-- clean MASLD_DX_CODES cluster.
--
-- Implemented from the ECL-managed SLD_DX_CODES cluster
-- (<< 197321007 OR << 79720007 OR << 29185008, with historic expansion;
-- verified equivalent to the EMIS NAFLD v2 valueset expansion).

with
nafld_codes as (
    select distinct person_id
    from ({{ get_observations("'SLD_DX_CODES'") }})
),

active as (
    select person_id from {{ ref('dim_person_active_patients') }}
)

select distinct n.person_id
from nafld_codes n
inner join active a on n.person_id = a.person_id
