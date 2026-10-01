{{ config(materialized='table') }}

-- LTC LCS CKD case finding: ICB_CF_CKD_61_1D_woEX population (pre-Rule-1 helper)
-- Canon: docs/emis_specs/ltc_lcs_r5/casefinding/specs/conditions/ckd/icb-cf-ckd-61-1d.md
--
-- From ICB_CF_CKD_61_BASE, include patients with an eGFR > 0 recorded in vs1 (8 codes)
--   AND whose LATEST eGFR (vs2, 6 codes; value > 0) is < 60.
-- Canon reads "...vs1 where Value > 0 AND vs2 where Value > 0 then Latest 1 where value < 60":
--   vs1 is an existence precondition (any eGFR > 0); the latest-value < 60 test runs on vs2.
--   The "2" in the report title is legacy naming - the rule is Latest 1, not two consecutive lows.
-- Collision: both vs1 ids share the friendly name; vs2 has a distinct name. The vs1 used here
--   is the 8-code woEX existence set (pinned id), NOT the 1-code Rule-1 screening marker.
--
-- Consumed by int_ltc_lcs_cf_ckd_61 (adds Rule 1) and by 62/63/64 woEX mutual exclusions.

with base as (
    select person_id
    from {{ ref('int_ltc_lcs_cf_ckd_61_base') }}
),

-- vs1 (8-code woEX set, pinned id): existence of an eGFR result > 0
egfr_recorded as (
    select person_id
    from ({{ get_ltc_lcs_observations("cc380a59-cc4c-ee42-4a2e-30ab9286dc71") }})
    where result_value > 0
),

-- vs2 (6 codes): latest eGFR per person, require value < 60
latest_egfr_low as (
    select person_id
    from ({{ get_ltc_lcs_observations_latest("with_most_recent_2_egfr60_and_not_on_ckd_reg_vs2") }})
    where result_value > 0
        and result_value < 60
)

select distinct b.person_id
from base as b
inner join egfr_recorded as e
    on b.person_id = e.person_id
inner join latest_egfr_low as l
    on b.person_id = l.person_id
