-- LTC LCS: COPD register
-- EMIS source: 'LTC LCS: COPD Register*' (R6 shared 11 November 2026)
--
-- The LTC LCS COPD register differs from QOF by also keeping COPD_COD-coded
-- patients whose earliest code is a diagnosis (rules 2 and 6). Those rules
-- carry the same active-COPD group requirement: no COPD resolved code at
-- all, OR a diagnosis code dated after the latest resolved code.
--
-- Rule chain (include on pass, all rules):
-- - Rule 1: R6 replaces library item ee5b135f with inline QOF v51 evidence:
--   COPDDIAG_COD at any date, COPDPROC_COD in the last two years, or either
--   after the latest COPDRES_COD. Its two criteria split the earliest evidence
--   at 1 April 2023 with no spirometry test, so together they select anyone
--   with evidence. Implemented as the QOF v51 COPD register.
-- - Rule 2: earliest COPD-family code (COPD_COD + COPDRES_COD) dated before
--   1 year ago is a diagnosis (COPD_COD), and COPD is active
-- - Rule 3: R6 uses rule 1's evidence on or after 1 April 2023 with linked
--   spirometry, so rule 1 already includes everyone it selects. Not implemented.
-- - Rules 4-5: newly registered variants; subsumed by rule 6 (their first
--   criterion alone passes rule 6) and not implemented
-- - Rule 6: earliest COPD-family code within the last year is a diagnosis,
--   and COPD is active
--
-- Implemented with the PCD COPD_COD / COPDRES_COD clusters (same refsets the
-- EMIS search references).

with
copd_family as (
    select person_id, id as observation_id, clinical_effective_date, cluster_id
    from ({{ get_observations("'COPD_COD','COPDRES_COD'") }})
),

active as (
    select person_id from {{ ref('dim_person_active_patients') }}
),

-- Active COPD: no resolved code, or a diagnosis dated after the latest
-- resolved code (the group requirement on every register rule)
latest_resolved as (
    select person_id, max(clinical_effective_date) as latest_resolved_date
    from copd_family
    where cluster_id = 'COPDRES_COD'
    group by person_id
),

copd_active as (
    select distinct f.person_id
    from copd_family f
    left join latest_resolved r on f.person_id = r.person_id
    where f.cluster_id = 'COPD_COD'
      and (
          r.latest_resolved_date is null
          or f.clinical_effective_date > r.latest_resolved_date
      )
),

-- Rule 2: earliest family code dated before 1 year ago is a diagnosis
rule_2_established_diagnosis as (
    select person_id
    from copd_family
    where clinical_effective_date < dateadd(year, -1, current_date())
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date asc, observation_id asc
    ) = 1
        and cluster_id = 'COPD_COD'
),

-- Rule 6: earliest family code within the last year is a diagnosis
rule_6_recent_diagnosis as (
    select person_id
    from copd_family
    where clinical_effective_date >= dateadd(year, -1, current_date())
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date asc, observation_id asc
    ) = 1
        and cluster_id = 'COPD_COD'
),

-- Rule 1: QOF v51 COPD register (R6 inline rule 1)
rule_1_qof_register as (
    select person_id
    from {{ ref('fct_person_copd_register') }}
    where is_on_register = true
)

select distinct u.person_id
from (
    select person_id from rule_1_qof_register
    union
    select person_id
    from (
        select person_id from rule_2_established_diagnosis
        union
        select person_id from rule_6_recent_diagnosis
    )
    where person_id in (select person_id from copd_active)
) u
inner join active a on u.person_id = a.person_id
