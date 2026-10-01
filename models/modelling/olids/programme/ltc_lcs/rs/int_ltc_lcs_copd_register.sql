-- LTC LCS: COPD register
-- EMIS source: 'LTC LCS: COPD Register*'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/copd/ltc_lcs_copd_register.md)
--
-- The LTC LCS COPD register differs from QOF by also keeping patients coded
-- in the last year ahead of spirometry confirmation (rules 3-6). Every rule
-- carries the same active-COPD group requirement: no COPD resolved code at
-- all, OR a diagnosis code dated after the latest resolved code.
--
-- Rule chain (include on pass, all rules):
-- - Rule 1: EMIS library item ee5b135f-b9b2-4ef7-8b51-939a754cf935 (not in the
--   XML export; assumed to be the QOF COPD register - empirically adds < 10
--   people over rules 2-6)
-- - Rule 2: earliest COPD-family code (COPD_COD + COPDRES_COD) dated before
--   1 year ago is a diagnosis (COPD_COD), and COPD is active
-- - Rule 3: COPD-family code in last year with linked spirometry
--   (FEV1/FVC < 0.7 or FEV1FVCL70 code) within -93/+186 days, and COPD is active
-- - Rules 4-5: newly registered variants; subsumed by rule 6 (their first
--   criterion alone passes rule 6) and not implemented
-- - Rule 6: earliest COPD-family code within the last year is a diagnosis,
--   and COPD is active
--
-- Implemented with the PCD COPD_COD / COPDRES_COD / FEV1FVC_COD /
-- FEV1FVCL70_COD clusters (same refsets the EMIS search references).

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

-- Rule 3: family code in last year with spirometry within -93/+186 days
spirometry as (
    select person_id, clinical_effective_date
    from ({{ get_observations("'FEV1FVC_COD'") }})
    where result_value < 0.7
    union all
    select person_id, clinical_effective_date
    from ({{ get_observations("'FEV1FVCL70_COD'") }})
),

rule_3_confirmed_recent as (
    select distinct f.person_id
    from copd_family f
    inner join spirometry s
        on f.person_id = s.person_id
        and s.clinical_effective_date
            between dateadd(day, -93, f.clinical_effective_date)
            and dateadd(day, 186, f.clinical_effective_date)
    where f.clinical_effective_date >= dateadd(year, -1, current_date())
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

-- Rule 1: QOF COPD register (assumed library item)
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
        select person_id from rule_3_confirmed_recent
        union
        select person_id from rule_6_recent_diagnosis
    )
    where person_id in (select person_id from copd_active)
) u
inner join active a on u.person_id = a.person_id
