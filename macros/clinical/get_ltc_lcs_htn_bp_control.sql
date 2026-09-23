{% macro get_ltc_lcs_htn_bp_control(window_start, window_end) %}
-- LTC LCS Outcomes: hypertension good blood pressure control (window-parameterised).
-- EMIS source: [ICS_HTN_21 v3_NICE] [*OC] % of people on HTN register with good controlled (2),
-- with parent searches B/C/E/H/I and B/C/E/H/I_EXFr.
--
-- Denominator: currently registered people on the hypertension register, excluding:
--   - age >= 90
--   - end of life care code, ever (htn_reg_without_outcome_exclusions_vs1)
--   - lives in care home code, ever (htn_reg_without_outcome_exclusions_vs2)
--   - latest MILDFRAIL/MODFRAIL/SEVFRAIL record is the "Moderate frailty" or "Severe frailty"
--     finding. EMIS tests only these two codes, so a latest Clinical Frailty Scale code does
--     not exclude.
-- EMIS library item ea06414e-6bec-4593-837f-5b854c54a8c7 (OR'd with end of life care) is not
-- in the XML export and is omitted pending EMIS verification.
--
-- Numerator: last known paired BP in the window is below the NICE target for its context:
--   - age < 80:  clinic < 140/90, home/ABPM < 135/85
--   - age >= 80: clinic < 150/90, home/ABPM < 145/85
-- The EMIS linked-record chain (latest CLINBP_COD or HOMEAMBBP_COD event, then its same-date
-- systolic and diastolic) is represented by int_blood_pressure_all's paired readings.
--
-- window_start / window_end are inclusive SQL date expressions, so the same logic serves the
-- rolling and FY variants.
with hypertension_register as (
    select
        hr.person_id,
        hr.age
    from {{ ref('fct_person_hypertension_register') }} as hr
    inner join {{ ref('dim_person_active_patients') }} as ap
        on hr.person_id = ap.person_id
    where hr.is_on_register = true
),

end_of_life_care as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("htn_reg_without_outcome_exclusions_vs1") }})
),

care_home as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("htn_reg_without_outcome_exclusions_vs2") }})
),

-- Latest coded frailty record; a same-date moderate/severe finding wins the tie.
latest_frailty as (
    select
        person_id,
        concept_code in (
            '925831000000107', -- Moderate frailty
            '925861000000102'  -- Severe frailty
        ) as is_moderate_or_severe_finding
    from {{ ref('int_frailty_diagnoses_all') }}
    where clinical_effective_date <= current_date()
    qualify row_number() over (
        partition by person_id
        order by clinical_effective_date desc, is_moderate_or_severe_finding desc, id desc
    ) = 1
),

denominator as (
    select hr.*
    from hypertension_register as hr
    left join latest_frailty as lf
        on hr.person_id = lf.person_id
    where hr.age < 90
        and hr.person_id not in (select person_id from end_of_life_care)
        and hr.person_id not in (select person_id from care_home)
        and not coalesce(lf.is_moderate_or_severe_finding, false)
),

-- Last known paired reading within the window. Tie-break mirrors int_blood_pressure_latest
-- (lowest-of-day) so a multi-reading final date resolves deterministically.
bp_in_window as (
    select
        person_id,
        effective_date as latest_bp_date,
        systolic_value as latest_systolic_value,
        diastolic_value as latest_diastolic_value,
        is_home_bp_event,
        is_abpm_bp_event
    from {{ ref('int_blood_pressure_all') }}
    where effective_date >= ({{ window_start }})
      and effective_date <= ({{ window_end }})
      and systolic_value is not null
      and diastolic_value is not null
    qualify row_number() over (
        partition by person_id
        order by effective_date desc, systolic_value asc, diastolic_value asc
    ) = 1
),

control as (
    select
        d.person_id,
        d.age,
        bp.latest_bp_date,
        bp.latest_systolic_value,
        bp.latest_diastolic_value,
        bp.is_home_bp_event,
        bp.is_abpm_bp_event,
        coalesce(bp.is_home_bp_event or bp.is_abpm_bp_event, false) as is_home_or_abpm,
        (bp.person_id is not null) as has_bp_in_window
    from denominator as d
    left join bp_in_window as bp
        on d.person_id = bp.person_id
)

select
    person_id,
    age,
    latest_bp_date,
    latest_systolic_value,
    latest_diastolic_value,
    is_home_bp_event,
    is_abpm_bp_event,
    case
        when age >= 80 and is_home_or_abpm then 145
        when age >= 80 then 150
        when is_home_or_abpm then 135
        else 140
    end as systolic_threshold,
    case when is_home_or_abpm then 85 else 90 end as diastolic_threshold,
    has_bp_in_window,
    -- Good control: both values strictly below target. People with no reading in the window
    -- are uncontrolled (false), flagged via has_bp_in_window.
    coalesce(
        latest_systolic_value < systolic_threshold
        and latest_diastolic_value < diastolic_threshold,
        false
    ) as is_bp_controlled
from control
{% endmacro %}
