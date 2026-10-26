{% macro get_ltc_lcs_htn_bp_control(window_start, window_end) %}
-- LTC LCS Outcomes: hypertension good blood pressure control (window-parameterised).
-- EMIS source: [ICS_HTN_21 v3_NICE] [*OC] % of people on HTN register with good controlled (2),
-- with parent searches B/C/E/H/I and B/C/E/H/I_EXFr.
--
-- Denominator: people on the hypertension register, excluding:
--   - age >= 90
--   - end of life care code, ever (htn_reg_without_outcome_exclusions_vs1)
--   - lives in care home code, ever (htn_reg_without_outcome_exclusions_vs2)
--   - latest MILDFRAIL/MODFRAIL/SEVFRAIL record is the "Moderate frailty" or "Severe frailty"
--     finding. EMIS tests only these two codes, so a latest Clinical Frailty Scale code does
--     not exclude.
-- EMIS library item ea06414e-6bec-4593-837f-5b854c54a8c7 (OR'd with end of life care) is not
-- in the XML export and is omitted pending EMIS verification.
--
-- Numerator: latest date-linked clinic/home components meet strict NICE targets.
-- Components rank by clinical date, save time, then observation ID before links and tests.
-- Under 80: clinic <140/90, home/ABPM <135/85. At 80+: clinic <150/90, home/ABPM <145/85.
-- Both values must be >=1. An implausible latest reading never falls back to an older one.
--
-- The EMIS search also requires current registration. That is left to consumers, which
-- filter on is_active (e.g. the LTC LCS dashboard via ltc_lcs_risk_stratification_base).
--
-- window_start / window_end are inclusive SQL date expressions, so the same logic serves the
-- rolling and FY variants.
with hypertension_register as (
    select
        person_id,
        age
    from {{ ref('fct_person_hypertension_register') }}
    where is_on_register = true
),

end_of_life_care as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("htn_reg_without_outcome_exclusions_vs1") }})
),

care_home as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("htn_reg_without_outcome_exclusions_vs2") }})
),

-- Use source clinical dates, without get_observations' date substitution or missing-date sentinel.
latest_frailty as (
    select
        f.person_id,
        f.concept_code in (
            '925831000000107', -- Moderate frailty
            '925861000000102'  -- Severe frailty
        ) as is_moderate_or_severe_finding
    from {{ ref('int_frailty_diagnoses_all') }} as f
    inner join {{ ref('stg_olids_observation') }} as o on f.id = o.id
    where o.clinical_effective_date::date <= current_date()
    qualify row_number() over (
        partition by f.person_id
        order by o.clinical_effective_date::date desc, o.date_recorded desc nulls last, f.id desc
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

bp_evidence as (
    {{ get_ltc_lcs_bp_latest_by_setting(as_at=window_end) }}
),

control as (
    select
        d.person_id,
        d.age,
        -- Home and clinic use the selected chain's own values and dates.
        case when bp.is_latest_reading_home then bp.home_reading_date
            when bp.is_latest_reading_clinic then bp.clinic_reading_date end as selected_bp_date,
        case when bp.is_latest_reading_home then bp.home_systolic_value
            when bp.is_latest_reading_clinic then bp.clinic_systolic_value end as selected_systolic_value,
        case when bp.is_latest_reading_home then bp.home_diastolic_value
            when bp.is_latest_reading_clinic then bp.clinic_diastolic_value end as selected_diastolic_value,
        coalesce(bp.is_latest_reading_home, false) as is_home_or_abpm,
        coalesce(bp.is_home_reading_abpm, false) as is_selected_abpm,
        coalesce(
            (bp.is_latest_reading_clinic
                and bp.latest_reading_date >= ({{ window_start }})
                and bp.clinic_reading_date >= ({{ window_start }}))
            or (bp.is_latest_reading_home and bp.home_reading_date >= ({{ window_start }})),
            false
        ) as has_bp_in_window
    from denominator as d
    left join bp_evidence as bp on d.person_id = bp.person_id
)

select
    person_id,
    age,
    iff(has_bp_in_window, selected_bp_date, null) as latest_bp_date,
    iff(has_bp_in_window, selected_systolic_value, null) as latest_systolic_value,
    iff(has_bp_in_window, selected_diastolic_value, null) as latest_diastolic_value,
    has_bp_in_window and is_home_or_abpm and not is_selected_abpm as is_home_bp_event,
    has_bp_in_window and is_home_or_abpm and is_selected_abpm as is_abpm_bp_event,
    case
        when age >= 80 and is_home_or_abpm then 145
        when age >= 80 then 150
        when is_home_or_abpm then 135
        else 140
    end as systolic_threshold,
    case when is_home_or_abpm then 85 else 90 end as diastolic_threshold,
    has_bp_in_window,
    coalesce(
        has_bp_in_window
        and latest_systolic_value >= 1 and latest_systolic_value < systolic_threshold
        and latest_diastolic_value >= 1 and latest_diastolic_value < diastolic_threshold,
        false
    ) as is_bp_controlled
from control
{% endmacro %}
