{{ config(materialized='table') }}

-- LTC LCS CKD case finding: ICB_CF_CKD_63_1D_woEX population (pre-Rule-1 helper)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/ckd/icb-cf-ckd-63-1d.md
--
-- From ICB_CF_CKD_61_BASE, EXCLUDE everyone in ICB_CF_CKD_61_1D_woEX AND ICB_CF_CKD_62_woEX,
--   then include patients whose LATEST UACR (vs1, 1 code; value > 0) is > 70.
--
-- Consumed by int_ltc_lcs_cf_ckd_63 (adds Rule 1) and by 64 woEX mutual exclusion.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_61_base') }}
),

ckd_61_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_61_woEX') }}
),

ckd_62_woex as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_62_woEX') }}
),

-- vs1 (1 code): latest UACR per person, require value > 70
latest_uacr_severe as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("with_most_recent_uacr70_and_not_on_ckd_reg_vs1") }})
    where result_value > 0
        and result_value > 70
)

select distinct b.person_id
from base as b
inner join latest_uacr_severe as u
    on b.person_id = u.person_id
where b.person_id not in (select person_id from ckd_61_woex)
    and b.person_id not in (select person_id from ckd_62_woex)
