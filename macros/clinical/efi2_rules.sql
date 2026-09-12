{% macro efi2_rules(
    cohort_relation,
    haemoglobin_relation,
    historical=false,
    deduplicate_output=false,
    evidence_tiebreak_expression=none
) %}
{# Shared eFI2 deficit rules for current and monthly scoring. #}

with
    rules_needed as (
        select *
        from {{ cohort_relation }}
        {% if historical and is_incremental() %}
        where {{ rebuild_month_window('end_date') }}
        {% endif %}
    ),

    -- - HYPERTENSIONS
    systolic_hypertension_clinic as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value >= 140) > 0 as has_deficit,
            'CLINIC_SYSTOLIC_HTN' as sub_deficit,
            end_date,
            max(case when result_value >= 140 then clinical_effective_date end) as last_date
        from rules_needed
        where
            regexp_like(other_instructions, '3 reading([s]?) equal to or greater than 140.*')
            and deficit = 'Hypertension'
        group by person_id, deficit, other_instructions, end_date
    ),

    systolic_hypertension_home as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value >= 135) > 0 as has_deficit,
            'HOME_SYSTOLIC_HTN' as sub_deficit,
            end_date,
            max(case when result_value >= 135 then clinical_effective_date end) as last_date
        from rules_needed
        where other_instructions = 'Equal to or greater than 135' and deficit = 'Hypertension'
        group by person_id, deficit, other_instructions, end_date
    ),

    diastolic_hypertension_clinic as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value >= 90) > 0 as has_deficit,
            'CLINIC_DIASTOLIC_HTN' as sub_deficit,
            end_date,
            max(case when result_value >= 90 then clinical_effective_date end) as last_date
        from rules_needed
        where
            other_instructions = '3 readings equal to or greater than 90 EVER'
            and deficit = 'Hypertension'
        group by person_id, deficit, other_instructions, end_date
    ),

    diastolic_hypertension_home as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value > 85) > 0 as has_deficit,
            'HOME_DIASTOLIC_HTN' as sub_deficit,
            end_date,
            max(case when result_value > 85 then clinical_effective_date end) as last_date
        from rules_needed
        where other_instructions = 'Equal to or greater than 85' and deficit = 'Hypertension'
        group by person_id, deficit, other_instructions, end_date
    ),

    activity_limitation as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if((result_value between 0 and 18) or (result_value between 21 and 90))
            > 0 as has_deficit,
            'BARTHEL_ACTIVITY_LIMITATION' as sub_deficit,
            end_date,
            max(
                case
                    when (result_value between 0 and 18) or (result_value between 21 and 90)
                    then clinical_effective_date
                end
            ) as last_date
        from rules_needed
        where deficit = 'Activity limitation'
        group by person_id, deficit, other_instructions, end_date
    ),

    weekly_alcohol_intake as (
        select
            person_id,
            deficit,
            case
                when result_value = 0
                then 'Zero alcohol'
                when result_value between 1 and 20
                then 'Lower risk drinking'
                when result_value between 21 and 48
                then 'Higher risk drinking'
                when result_value >= 49
                then 'Harmful drinking'
            end as other_instructions,
            -- Per-reading risk flag: higher-risk (21-48) or harmful (49+) weekly units.
            -- Alcohol scoring is not driven by this flag — int_efi2_chronology derives
            -- the HARMFUL / PREVIOUS_HIGHER_HARMFUL state from the band labels and their
            -- recency. It is retained so the alcohol rows carry the has_deficit column
            -- the union projects, and as a meaningful risk indicator on the rules output.
            result_value >= 21 as has_deficit,
            'WEEKLY_UNITS' as sub_deficit,
            end_date,
            clinical_effective_date as last_date
        from rules_needed
        where
            other_instructions
            = 'Alcohol - numeric (0 = code as zero alcohol; 1-20 = code as lower risk drinking; 21-48 = code as higher risk drinking; 49+ = code as harmful drinking)'
            and deficit = 'Alcohol'
    ),

    daily_alcohol_intake as (
        select
            person_id,
            deficit,
            case
                when result_value * 7 = 0
                then 'zero alcohol'
                when result_value * 7 between 1 and 20
                then 'Lower risk drinking'
                when result_value * 7 between 21 and 48
                then 'Higher risk drinking'
                when result_value * 7 >= 49
                then 'Harmful drinking'
            end as other_instructions,
            -- Per-reading risk flag; daily units annualised to weekly (x 7) before
            -- banding. See weekly_alcohol_intake — alcohol scoring is derived in
            -- int_efi2_chronology, not from this flag.
            result_value * 7 >= 21 as has_deficit,
            'DAILY_UNITS' as sub_deficit,
            end_date,
            clinical_effective_date as last_date
        from rules_needed
        where
            other_instructions
            = 'Alcohol - numeric (0 = code as zero alcohol; 1-20 = code as lower risk drinking; 21-48 = code as higher risk drinking; 49+ = code as harmful drinking). Multiply by 7 to get weekly units.'
            and deficit = 'Alcohol'
    ),

    anaemia_code as (
        select
            person_id,
            deficit,
            other_instructions,
            count(result_value) > 0 as has_deficit,
            'ANAEMIA' as sub_deficit,
            end_date,
            gender,
            max(case when result_value > 0 then clinical_effective_date end) as last_date
        from rules_needed
        where deficit = 'Anaemia & haematinic deficiency'
        group by person_id, deficit, other_instructions, end_date, gender
    ),

    {% if historical %}
    haemoglobin_as_of as (
        select
            ac.person_id,
            ac.end_date,
            hb.inferred_value
        from anaemia_code ac
        left join {{ haemoglobin_relation }} hb
            on ac.person_id = hb.person_id
            and hb.clinical_effective_date <= ac.end_date
            and hb.inferred_value is not null
            and not hb.is_negative
            and not hb.is_extreme_outlier
        qualify row_number() over (
            partition by ac.person_id, ac.end_date
            order by hb.clinical_effective_date desc, hb.id desc
        ) = 1
    ),
    {% endif %}

    anaemia as (
        select distinct
            ac.person_id,
            ac.deficit,
            ac.other_instructions,
            -- Reuse-native anaemia gate. lds joined base_phenolab__dev_measurements
            -- (Hb in g/dL, thresholds 13 male / 12 female). Here we use the repo's
            -- haemoglobin model (standardised to g/L), so the equivalent
            -- thresholds are 130 male / 120 female. Persons with no Hb measurement
            -- are kept, matching lds's effective behaviour (its OR-null /
            -- OR-older-than-last-code branches made the Hb gate near-passthrough).
            coalesce(
                case
                    when ac.gender = 'MALE' and hb.inferred_value < 130 then true
                    when ac.gender = 'FEMALE' and hb.inferred_value < 120 then true
                    when hb.inferred_value is null then true
                    else false
                end,
                true
            ) as has_deficit,
            ac.sub_deficit,
            ac.end_date,
            ac.last_date
        from anaemia_code ac
        left join {% if historical %}haemoglobin_as_of{% else %}{{ haemoglobin_relation }}{% endif %} hb
            on ac.person_id = hb.person_id
            {% if historical %}and ac.end_date = hb.end_date{% endif %}
    ),

    atrial_fibrillation as (
        select
            person_id,
            deficit,
            other_instructions,
            count(result_value) > 0 as has_deficit,
            'AF' as sub_deficit,
            end_date,
            max(case when result_value > 0 then clinical_effective_date end) as last_date
        from rules_needed
        where deficit = 'Atrial fibrillation'
        group by person_id, deficit, other_instructions, end_date
    ),

    last_bmi as (
        select
            person_id,
            deficit,
            other_instructions,
            result_value,
            -- Most recent BMI reading = rn 1. lds ordered by age_at_event ascending,
            -- which picked the EARLIEST reading; the paper uses current BMI, so we
            -- order by clinical_effective_date descending. Paper-alignment deviation.
            row_number() over (
                partition by person_id, end_date
                order by clinical_effective_date desc nulls last
                    {% if evidence_tiebreak_expression is not none %}
                    , {{ evidence_tiebreak_expression }} desc
                    {% endif %}
            ) as rn,
            'BMI' as sub_deficit,
            end_date,
            clinical_effective_date
        from rules_needed
        where deficit = 'BMI'
    ),

    bmi as (
        select
            person_id,
            deficit,
            other_instructions,
            case
                when result_value between 30 and 250
                then true
                when result_value between 7 and 18.5
                then true
                else false
            end as has_deficit,
            case
                when result_value between 30 and 250
                then 'Obese'
                when result_value between 7 and 18.5
                then 'Underweight'
                else null
            end as sub_deficit,
            end_date,
            clinical_effective_date as last_date
        from last_bmi
        where deficit = 'BMI' and rn = 1
    ),

    ckd_urine_prot as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value >= 150) > 0 as has_deficit,
            'CKD_URINE_PROTEIN' as sub_deficit,
            end_date,
            max(case when result_value >= 150 then clinical_effective_date end) as last_date
        from rules_needed
        where
            deficit = 'Chronic kidney disease'
            and other_instructions = 'Any abnormal result (>150mg/24hr if not specified in dataset)'
        group by person_id, deficit, other_instructions, end_date
    ),

    ckd_urine_alb as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value >= 20) > 0 as has_deficit,
            'CKD_URINE_ALBUMIN' as sub_deficit,
            end_date,
            max(case when result_value >= 20 then clinical_effective_date end) as last_date
        from rules_needed
        where
            deficit = 'Chronic kidney disease'
            and other_instructions = 'Any abnormal result (>20mg/24hr if not specified in dataset)'
        group by person_id, deficit, other_instructions, end_date
    ),

    ckd_urine_acr as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value >= 3) > 0 as has_deficit,
            'CKD_URINE_ALBUMIN_CR_RATIO' as sub_deficit,
            end_date,
            max(case when result_value >= 3 then clinical_effective_date end) as last_date
        from rules_needed
        where
            deficit = 'Chronic kidney disease'
            and other_instructions = 'Any abnormal result (>3mg/mmol if not specified in dataset)'
        group by person_id, deficit, other_instructions, end_date
    ),

    ckd_urine_pcr as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value >= 50) > 0 as has_deficit,
            'CKD_URINE_PROTEIN_CR_RATIO' as sub_deficit,
            end_date,
            max(case when result_value >= 50 then clinical_effective_date end) as last_date
        from rules_needed
        where
            deficit = 'Chronic kidney disease'
            and other_instructions = 'Any abnormal result (>50mg/mmol if not specified in dataset)'
        group by person_id, deficit, other_instructions, end_date
    ),

    ckd_egfr as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value < 60) > 0 as has_deficit,
            'CKD_EGFR' as sub_deficit,
            end_date,
            max(case when result_value < 60 then clinical_effective_date end) as last_date
        from rules_needed
        where deficit = 'Chronic kidney disease' and other_instructions = 'If less than 60'
        group by person_id, deficit, other_instructions, end_date
    ),

    cognitive_score as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value >= 8) > 0 as has_deficit,
            'COGNITIVE_SCORE' as sub_deficit,
            end_date,
            max(case when result_value >= 8 then clinical_effective_date end) as last_date
        from rules_needed
        where
            deficit = 'Cognitive impairment'
            and other_instructions = 'If equal to or greater than 8'
        group by person_id, deficit, other_instructions, end_date
    ),

    cognitive_impairment as (
        select
            person_id,
            deficit,
            other_instructions,
            count(*) > 0 as has_deficit,
            'COGNITIVE_IMPAIRMENT' as sub_deficit,
            end_date,
            max(case when result_value > 0 then clinical_effective_date end) as last_date
        from rules_needed
        where
            deficit = 'Cognitive impairment'
            and other_instructions = 'Removed if dementia code added'
        group by person_id, deficit, other_instructions, end_date
    ),

    falls as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value > 0) > 0 as has_deficit,
            'FALLS' as sub_deficit,
            end_date,
            max(case when result_value > 0 then clinical_effective_date end) as last_date
        from rules_needed
        where deficit = 'Falls'
        group by person_id, deficit, other_instructions, end_date
    ),

    fracture as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value > 0) > 0 as has_deficit,
            'FRACTURE' as sub_deficit,
            end_date,
            max(case when result_value > 0 then clinical_effective_date end) as last_date
        from rules_needed
        where deficit = 'Fracture'
        group by person_id, deficit, other_instructions, end_date
    ),

    diastolic_hypotension as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value < 60) >= 3 as has_deficit,
            'DIASTOLIC_HYPOTENSION' as sub_deficit,
            end_date,
            max(case when result_value < 60 then clinical_effective_date end) as last_date
        from rules_needed
        where
            deficit = 'Hypotension / syncope'
            and regexp_like(other_instructions, '3 readings less than 60.*')
        group by person_id, deficit, other_instructions, end_date
    ),

    systolic_hypotension as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value < 90) >= 3 as has_deficit,
            'SYSTOLIC_HYPOTENSION' as sub_deficit,
            end_date,
            max(case when result_value < 90 then clinical_effective_date end) as last_date
        from rules_needed
        where
            deficit = 'Hypotension / syncope'
            and other_instructions = '3 readings less than 90 EVER'
        group by person_id, deficit, other_instructions, end_date
    ),

    memory_concerns as (
        select
            person_id,
            deficit,
            other_instructions,
            count(*) > 0 as has_deficit,
            'MEMORY_CONCERNS' as sub_deficit,
            end_date,
            max(case when result_value > 0 then clinical_effective_date end) as last_date
        from rules_needed
        where deficit = 'Memory concerns'
        group by person_id, deficit, other_instructions, end_date
    ),

    osteoporosis as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value < -2.5) > 0 as has_deficit,
            'OSTEOPOROSIS_FRAX' as sub_deficit,
            end_date,
            max(case when result_value < -2.5 then clinical_effective_date end) as last_date
        from rules_needed
        where deficit = 'Osteoporosis'
        group by person_id, deficit, other_instructions, end_date
    ),

    pvd as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value < 0.95) > 0 as has_deficit,
            'PERIPHERAL_VASCULAR_DISEASE' as sub_deficit,
            end_date,
            max(case when result_value < 0.95 then clinical_effective_date end) as last_date
        from rules_needed
        where deficit = 'Peripheral vascular disease' and other_instructions = 'If less than 0.95'
        group by person_id, deficit, other_instructions, end_date
    ),

    skin_ulcer_any as (
        select
            person_id,
            deficit,
            other_instructions,
            count(result_value) > 0 as has_deficit,
            'SKIN_ULCER_ANY' as sub_deficit,
            end_date,
            max(case when result_value > 0 then clinical_effective_date end) as last_date
        from rules_needed
        where deficit = 'Skin ulcer' and other_instructions = 'Any value'
        group by person_id, deficit, other_instructions, end_date
    ),

    skin_ulcer_count as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value > 0) > 0 as has_deficit,
            'SKIN_ULCER_COUNT' as sub_deficit,
            end_date,
            max(case when result_value > 0 then clinical_effective_date end) as last_date
        from rules_needed
        where deficit = 'Skin ulcer' and other_instructions = 'If greater than 0'
        group by person_id, deficit, other_instructions, end_date
    ),

    smoking_current as (
        select
            person_id,
            deficit,
            other_instructions,
            count(result_value) > 0 as has_deficit,
            'SMOKING_CURRENT' as sub_deficit,
            end_date,
            max(case when result_value > 0 then clinical_effective_date end) as last_date
        from rules_needed
        where
            deficit = 'Smoker (current)' and other_instructions = 'Cannot be current and ex smoker'
        group by person_id, deficit, other_instructions, end_date
    ),

    smoking_ex as (
        select
            person_id,
            deficit,
            other_instructions,
            count(result_value) > 0 as has_deficit,
            'SMOKING_EX' as sub_deficit,
            end_date,
            max(case when result_value > 0 then clinical_effective_date end) as last_date
        from rules_needed
        where deficit = 'Smoker (ex)' and other_instructions = 'Cannot be ex and current smoker'
        group by person_id, deficit, other_instructions, end_date
    ),

    thyroid_problems as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value not between 0.36 and 5.5) > 0 as has_deficit,
            'THYROID_DISEASE' as sub_deficit,
            end_date,
            max(
                case when result_value not between 0.36 and 5.5 then clinical_effective_date end
            ) as last_date
        from rules_needed
        where deficit = 'Thyroid problems'
        group by person_id, deficit, other_instructions, end_date
    ),

    urinary_incontinence as (
        select
            person_id,
            deficit,
            other_instructions,
            count(result_value) > 0 as has_deficit,
            'URINARY_INCONTINENCE' as sub_deficit,
            end_date,
            max(case when result_value > 0 then clinical_effective_date end) as last_date
        from rules_needed
        where
            deficit = 'Urinary incontinence'
            and other_instructions = 'If code recorded age>18'
            and age_at_event > 18
        group by person_id, deficit, other_instructions, end_date
    ),

    weight_loss as (
        select
            person_id,
            deficit,
            other_instructions,
            count_if(result_value >= 1) > 0 as has_deficit,
            'WEIGHT_LOSS' as sub_deficit,
            end_date,
            max(case when result_value >= 1 then clinical_effective_date end) as last_date
        from rules_needed
        where
            deficit = 'Weight loss'
            and other_instructions = 'If equal to or greater than 1'
            and age_at_event > 18
        group by person_id, deficit, other_instructions, end_date
    ),

    unioned as (

        -- Now add back the ones that didn't have instructions
        select
            person_id,
            deficit,
            other_instructions,
            true as has_deficit,
            deficit as sub_deficit,
            end_date,
            clinical_effective_date as last_date
        from rules_needed
        where other_instructions is null and deficit <> 'BMI' -- high BMI passthrough - 95% missing label

        union all

        -- And all the ones with instructions
        {% for rule_cte in [
            "systolic_hypertension_clinic",
            "diastolic_hypertension_clinic",
            "systolic_hypertension_home",
            "diastolic_hypertension_home",
            "activity_limitation",
            "anaemia",
            "atrial_fibrillation",
            "daily_alcohol_intake",
            "weekly_alcohol_intake",
            "bmi",
            "ckd_urine_prot",
            "ckd_urine_alb",
            "ckd_urine_acr",
            "ckd_urine_pcr",
            "ckd_egfr",
            "cognitive_score",
            "cognitive_impairment",
            "falls",
            "fracture",
            "diastolic_hypotension",
            "systolic_hypotension",
            "memory_concerns",
            "osteoporosis",
            "pvd",
            "skin_ulcer_any",
            "skin_ulcer_count",
            "smoking_current",
            "smoking_ex",
            "thyroid_problems",
            "urinary_incontinence",
            "weight_loss",
        ] %}
            select
                person_id,
                deficit,
                other_instructions,
                has_deficit,
                sub_deficit,
                end_date,
                last_date
            from {{ rule_cte }} {{ "union all" if not loop.last }}

        {% endfor %}
    )

select {% if deduplicate_output %}distinct {% endif %}
    person_id,
    deficit,
    other_instructions,
    has_deficit,
    sub_deficit,
    end_date,
    last_date
from unioned
{% endmacro %}
