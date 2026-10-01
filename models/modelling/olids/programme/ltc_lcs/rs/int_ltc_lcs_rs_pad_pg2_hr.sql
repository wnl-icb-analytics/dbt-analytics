-- LTC LCS: PAD Register - Priority Group 2 (High Risk)
-- Parent population: PAD register, excluding PG1 (HRC)
-- EMIS source: 'On PAD Register- LTC LCS Priority Group 2 (HR)'
-- (docs/emis_specs/ltc_lcs_r5/risk_stratification/specs/conditions/pad/on_pad_register_ltc_lcs_priority_group_2_hr.md)
--
-- Rule chain:
-- - Rule 1 (gate): excludes PG1 (HRC) patients
-- - Rule 2: peripheral ischaemia code (vs1) dated 90-365 days ago -> include
-- - Rule 3 (gate): peripheral ischaemia code before 12 months ago. Fail -> exclude.
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
-- Parent population: Patients currently on PAD register
pad_register as (
    select distinct person_id
    from {{ ref('fct_person_pad_register') }}
    where is_on_register = true
),

-- Rule 1: Exclude PG1 (HRC)
pg1_exclusions as (
    select distinct person_id
    from {{ ref('int_ltc_lcs_rs_pad_pg1_hrc') }}
),

-- Rule 2: peripheral ischaemia code dated 90-365 days before run date
rule_2_recent_ischaemia as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_pad_reg_pg2_hr_vs1") }})
    where clinical_effective_date >= dateadd(day, -365, current_date())
        and clinical_effective_date < dateadd(day, -90, current_date())
),

-- Rule 3: peripheral ischaemia code before 12 months ago
rule_3_stale_ischaemia as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("on_pad_reg_pg2_hr_vs1") }})
    where clinical_effective_date < dateadd(month, -12, current_date())
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

-- Combine rule results for all PAD register patients
patient_rules as (
    select
        pr.person_id,
        (pg1.person_id is not null) as rule_1_in_pg1,
        (r2.person_id is not null) as rule_2_recent_ischaemia,
        (r3.person_id is not null) as rule_3_stale_ischaemia,
        (bpc.person_id is not null) as rules_4_8_bp_controlled,
        case
            when pg1.person_id is not null then 'Excluded'
            when r2.person_id is not null then 'Included'
            when r3.person_id is null then 'Excluded'
            when bpc.person_id is not null then 'Excluded'
            else 'Included'
        end as final_status
    from pad_register pr
    left join pg1_exclusions pg1 on pr.person_id = pg1.person_id
    left join rule_2_recent_ischaemia r2 on pr.person_id = r2.person_id
    left join rule_3_stale_ischaemia r3 on pr.person_id = r3.person_id
    left join bp_controlled bpc on pr.person_id = bpc.person_id
)

-- Final result: included patients only
select
    person_id,
    final_status,
    'PAD' as condition,
    '2' as priority_group,
    'HR' as risk_group
from patient_rules
where final_status = 'Included'
