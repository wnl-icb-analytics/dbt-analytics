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

-- Filter positive source-code results before Latest 1, then test > 70 below.
latest_uacr_severe as (
    select person_id, result_value
    from ({{ get_ltc_lcs_observations("6bfbf92c-3c89-cc89-f541-a7dcaa945562", match_paths='source', include_date_recorded=true) }})
    where result_value > 0
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc nulls last, date_recorded desc nulls last, observation_id desc
    ) = 1
)

select distinct b.person_id
from base as b
inner join latest_uacr_severe as u
    on b.person_id = u.person_id
where u.result_value > 70
    and b.person_id not in (select person_id from ckd_61_woex)
    and b.person_id not in (select person_id from ckd_62_woex)
