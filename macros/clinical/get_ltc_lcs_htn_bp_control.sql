{% macro get_ltc_lcs_htn_bp_control(window_start, window_end, reference='current') %}
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
-- Components link by date before ranking by clinical date, save time and observation ID.
-- Only the home-setting wrapper ranks before linking; thresholds test the selected values.
-- Under 80: clinic <140/90, home/ABPM <135/85. At 80+: clinic <150/90, home/ABPM <145/85.
-- Both values must be >=1. An implausible latest reading never falls back to an older one.
--
-- The EMIS search also requires current registration. That is left to consumers, which
-- filter on is_active (e.g. the LTC LCS dashboard via ltc_lcs_risk_stratification_base).
--
-- window_start / window_end are inclusive SQL date expressions, so the same logic serves the
-- rolling and FY variants.
--
-- reference='by_month' evaluates each of the last 60 completed month-ends instead. Register,
-- age and practice come from the monthly register and reference population, so only people
-- registered at the month-end are included. Exclusions use records dated on or before the
-- month-end. window_start may reference d.reporting_date; window_end is not used.
{% if reference not in ['current', 'by_month'] %}
    {{ exceptions.raise_compiler_error('Unsupported reference: ' ~ reference) }}
{% endif %}
{% set by_month = reference == 'by_month' %}
with hypertension_register as (
    {% if by_month %}
    select
        population.person_id,
        population.reporting_date,
        population.age,
        population.practice_code,
        population.practice_name
    from ({{ nice_register('HTN', 'by_month') }}) as register
    inner join ({{ nice_reference_population('by_month') }}) as population
        on register.person_id = population.person_id
        and register.reporting_date = population.reporting_date
    {% else %}
    select
        person_id,
        age
    from {{ ref('fct_person_hypertension_register') }}
    where is_on_register = true
    {% endif %}
),

{% if by_month %}
-- Earliest code date. An undated code applies at every month-end, as in the live "ever" rule.
end_of_life_care as (
    select person_id, min(coalesce(clinical_effective_date::date, '1900-01-01'::date)) as first_date
    from ({{ get_ltc_lcs_observations("htn_reg_without_outcome_exclusions_vs1") }})
    group by person_id
),

care_home as (
    select person_id, min(coalesce(clinical_effective_date::date, '1900-01-01'::date)) as first_date
    from ({{ get_ltc_lcs_observations("htn_reg_without_outcome_exclusions_vs2") }})
    group by person_id
),
{% else %}
end_of_life_care as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("htn_reg_without_outcome_exclusions_vs1") }})
),

care_home as (
    select distinct person_id
    from ({{ get_ltc_lcs_observations("htn_reg_without_outcome_exclusions_vs2") }})
),
{% endif %}

-- Use source clinical dates, without get_observations' date substitution or missing-date sentinel.
{% if by_month %}
-- One record per person and day (save time, then ID). The latest day on or before the
-- month-end gives the same record as ranking all records.
frailty_days as (
    select
        f.person_id,
        o.clinical_effective_date::date as frailty_date,
        f.concept_code in (
            '925831000000107', -- Moderate frailty
            '925861000000102'  -- Severe frailty
        ) as is_moderate_or_severe_finding
    from {{ ref('int_frailty_diagnoses_all') }} as f
    inner join {{ ref('stg_olids_observation') }} as o on f.id = o.id
    where o.clinical_effective_date::date <= current_date()
    qualify row_number() over (
        partition by f.person_id, o.clinical_effective_date::date
        order by o.date_recorded desc nulls last, f.id desc
    ) = 1
),

latest_frailty as (
    select
        hr.person_id,
        hr.reporting_date,
        fd.is_moderate_or_severe_finding
    from hypertension_register as hr
    asof join frailty_days as fd
        match_condition (hr.reporting_date >= fd.frailty_date)
        on hr.person_id = fd.person_id
),

denominator as (
    select hr.*
    from hypertension_register as hr
    left join latest_frailty as lf
        on hr.person_id = lf.person_id
        and hr.reporting_date = lf.reporting_date
    left join end_of_life_care as eol
        on hr.person_id = eol.person_id
        and eol.first_date <= hr.reporting_date
    left join care_home as ch
        on hr.person_id = ch.person_id
        and ch.first_date <= hr.reporting_date
    where hr.age < 90
        and eol.person_id is null
        and ch.person_id is null
        and not coalesce(lf.is_moderate_or_severe_finding, false)
),
{% else %}
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
{% endif %}

bp_evidence as (
    {% if by_month %}
    {{ get_ltc_lcs_bp_latest_by_setting(population='select person_id, reporting_date from denominator') }}
    {% elif window_end == 'current_date()' %}
    select * from {{ ref('int_ltc_lcs_bp_latest_by_setting') }}
    {% else %}
    {{ get_ltc_lcs_bp_latest_by_setting(as_at=window_end) }}
    {% endif %}
),

control as (
    select
        d.person_id,
        {% if by_month %}
        d.reporting_date,
        d.practice_code,
        d.practice_name,
        {% endif %}
        d.age,
        -- Home and clinic use the selected chain's own values and dates.
        case when bp.is_latest_reading_home then bp.home_reading_date
            when bp.is_latest_reading_clinic then bp.clinic_reading_date end as selected_bp_date,
        case when bp.is_latest_reading_home then bp.home_systolic_value
            when bp.is_latest_reading_clinic then bp.clinic_systolic_value end as selected_systolic_value,
        case when bp.is_latest_reading_home then bp.home_diastolic_value
            when bp.is_latest_reading_clinic then bp.clinic_diastolic_value end as selected_diastolic_value,
        coalesce(bp.is_home_reading_abpm, false) as is_selected_abpm,
        coalesce(
            (bp.is_latest_reading_clinic
                and bp.latest_reading_date >= ({{ window_start }})
                and bp.clinic_reading_date >= ({{ window_start }}))
            or (bp.is_latest_reading_home and bp.home_reading_date >= ({{ window_start }})),
            false
        ) as has_bp_in_window,
        has_bp_in_window and coalesce(bp.is_latest_reading_home, false) as is_home_or_abpm
    from denominator as d
    left join bp_evidence as bp
        on d.person_id = bp.person_id
        {% if by_month %}and d.reporting_date = bp.reporting_date{% endif %}
)

select
    person_id,
    {% if by_month %}
    reporting_date,
    practice_code,
    practice_name,
    {% endif %}
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
