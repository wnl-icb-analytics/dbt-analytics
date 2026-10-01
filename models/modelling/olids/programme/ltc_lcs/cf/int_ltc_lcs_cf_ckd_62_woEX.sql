{{ config(materialized='table') }}

-- LTC LCS CKD case finding: ICB_CF_CKD_62_woEX population (pre-Rule-1 helper)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/ckd/icb-cf-ckd-62.md
--
-- From ICB_CF_CKD_61_BASE, EXCLUDE everyone in ICB_CF_CKD_61_1D_woEX, then include
--   patients whose LATEST UACR (vs1, 1 code; value > 0) is > 4.
-- Canon: "...vs1 where Value > 0 AND vs1 where Value > 0 then Latest 1 where value > 4"
--   -> single latest UACR > 4 (no 2-high-reading count).
--
-- Consumed by int_ltc_lcs_cf_ckd_62 (adds Rule 1) and by 63/64 woEX mutual exclusions.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_61_base') }}
),

ckd_61_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_61_woEX') }}
),

-- vs1 (1 code): latest UACR per person, require value > 4
latest_uacr_high as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("with_most_recent_2_uacr4_and_not_on_ckd_reg_vs1") }})
    where result_value > 0
        and result_value > 4
)

select distinct b.person_id
from base as b
inner join latest_uacr_high as u
    on b.person_id = u.person_id
where b.person_id not in (select person_id from ckd_61_woex)
