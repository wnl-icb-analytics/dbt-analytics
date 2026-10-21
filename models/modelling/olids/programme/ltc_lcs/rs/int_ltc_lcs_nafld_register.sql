-- LTC LCS: NAFLD register
-- EMIS source: 'LTC LCS: NAFLD Register v2*'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/nafld/ltc_lcs_nafld_register_v2.md)
--
-- One row per currently registered, living, non-test person with any exported
-- NAFLD v2 diagnosis code ever recorded. No age or recency restriction.
-- Use the two XML code lists rather than the broader SLD_DX_CODES cluster.
-- EMIS-local codes match source codes exactly; public codes retain permitted descendants.

with
nafld_codes as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("08654d02-552a-7784-076c-e7fcc941d284,f0bfa8e2-972c-1cba-326f-573ef42e6124", emis_local_exact=true) }})
),

active as (
    select person_id from {{ ref('dim_person_active_patients') }}
)

select distinct n.person_id
from nafld_codes n
inner join active a on n.person_id = a.person_id
