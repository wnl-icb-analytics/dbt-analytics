-- LTC LCS: Stroke/TIA Register - Priority Group 2 (High Risk)
-- Parent population: Stroke/TIA register, excluding PG1 (HRC)
-- EMIS source: 'On Stroke/TIA Register- LTC LCS Priority Group 2 (HR)*'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/stroke_tia/on_stroke_tia_register_ltc_lcs_priority_group_2_hr.md)
--
-- Rule chain:
-- - Rule 1 (gate): excludes PG1 (HRC) patients
-- - Rule 2: stroke (vs1) or TIA (vs2) code dated 30-365 days ago with
--   Episode = First, New or Flare Up -> include.
--   The Problem Significance arm is a data gap (not in OLIDS).
-- - Rule 3 (gate): stroke/TIA code before 1 year ago. Fail -> exclude.
-- - Rules 4-8: exclude if latest BP in last 12 months is controlled, using
--   age-banded thresholds:
--   - age <= 80: clinic <= 140/90, home/ABPM <= 135/85
--   - age > 80: clinic <= 150/90, home/ABPM <= 145/85
--   Patients with no BP reading in 12 months, or an uncontrolled latest
--   reading, are included.
--
-- BP note: EMIS tests latest clinic and latest home/ABPM readings as separate
-- same-date linked-criteria chains; this port follows the established HTN rs
-- convention of testing the single latest BP event (int_blood_pressure_latest)
-- with thresholds chosen by reading type.

with
-- Parent population: Patients currently on Stroke/TIA register
stroke_tia_register as (
    select distinct person_id
    from {{ ref('fct_person_stroke_tia_register') }}
    where is_on_register = true
),

-- Rule 1: Exclude PG1 (HRC)
pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_stroke_pg1_hrc') }}
),

-- Rule 2: first/new or flare-up stroke/TIA code dated 30-365 days ago
rule_2_recent_stroke_episode as (
    select distinct o.person_id
    from ({{ get_ltc_lcs_observations("on_stroketia_reg_pg2_hr_vs1, on_stroketia_reg_pg2_hr_vs2") }}) o
    left join {{ ref('stg_olids_enriched_concept_map') }} ecm
        on o.episodicity_source_concept_id = ecm.source_concept_id
    where o.clinical_effective_date >= dateadd(day, -365, current_date())
        and o.clinical_effective_date < dateadd(day, -30, current_date())
        and ecm.source_display in ('First', 'New', 'Flare Up')
),

-- Rule 3: stroke/TIA code before 1 year ago
rule_3_stale_stroke as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_stroketia_reg_pg2_hr_vs1, on_stroketia_reg_pg2_hr_vs2") }})
    where clinical_effective_date < dateadd(year, -1, current_date())
),

-- Rules 4-8: latest BP within 12 months, controlled per age-banded thresholds
latest_bp as (
    select
        person_id,
        systolic_value,
        diastolic_value,
        coalesce(is_home_bp_event or is_abpm_bp_event, false) as is_home_or_abpm
    from {{ ref('int_blood_pressure_latest') }}
    where clinical_effective_date >= dateadd(month, -12, current_date())
),

ages as (
    select person_id, age
    from {{ ref('dim_person_age') }}
),

bp_controlled as (
    select bp.person_id
    from latest_bp bp
    left join ages a on bp.person_id = a.person_id
    where
        (
            coalesce(a.age, 0) <= 80
            and (
                (not bp.is_home_or_abpm
                    and bp.systolic_value between 1 and 140
                    and bp.diastolic_value between 1 and 90)
                or (bp.is_home_or_abpm
                    and bp.systolic_value between 1 and 135
                    and bp.diastolic_value between 1 and 85)
            )
        )
        or (
            coalesce(a.age, 0) > 80
            and (
                (not bp.is_home_or_abpm
                    and bp.systolic_value between 1 and 150
                    and bp.diastolic_value between 1 and 90)
                or (bp.is_home_or_abpm
                    and bp.systolic_value between 1 and 145
                    and bp.diastolic_value between 1 and 85)
            )
        )
),

-- Combine rule results for all Stroke/TIA register patients
patient_rules as (
    select
        sr.person_id,
        (pg1.person_id is not null) as rule_1_in_pg1,
        (r2.person_id is not null) as rule_2_recent_stroke_episode,
        (r3.person_id is not null) as rule_3_stale_stroke,
        (bpc.person_id is not null) as rules_4_8_bp_controlled,
        case
            when pg1.person_id is not null then 'Excluded'
            when r2.person_id is not null then 'Included'
            when r3.person_id is null then 'Excluded'
            when bpc.person_id is not null then 'Excluded'
            else 'Included'
        end as final_status
    from stroke_tia_register sr
    left join pg1_exclusions pg1 on sr.person_id = pg1.person_id
    left join rule_2_recent_stroke_episode r2 on sr.person_id = r2.person_id
    left join rule_3_stale_stroke r3 on sr.person_id = r3.person_id
    left join bp_controlled bpc on sr.person_id = bpc.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'Stroke/TIA' as condition,
    '2' as priority_group,
    'HR' as risk_group
from patient_rules
where final_status = 'Included'
