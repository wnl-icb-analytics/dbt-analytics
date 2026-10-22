with inclusion_list as (
    select patient_id
        , area_code
        , meets_original_criteria
        , meets_adults_with_complexity
    from {{ ref('cltcs_patient_list') }}
),

treatment_by_route as (
    select sk_patient_id
        , max(iff(inclusion_rule = 'original_criteria', score_treatment, null)) as score_treatment_original_criteria
        , max(iff(inclusion_rule = 'adults_with_complexity', score_treatment, null)) as score_treatment_adults_with_complexity
    from {{ ref('cltcs_score_treatment') }}
    group by sk_patient_id
),

nice_treatment_by_route as (
    select sk_patient_id
        , max(iff(inclusion_rule = 'original_criteria', score_treatment_age_adjusted, null)) as score_treatment_nice_original_criteria
        , max(iff(inclusion_rule = 'adults_with_complexity', score_treatment_age_adjusted, null)) as score_treatment_nice_adults_with_complexity
    from {{ ref('cltcs_score_treatment_pathway') }}
    group by sk_patient_id
),

frailty_by_route as (
    select sk_patient_id
        , max(iff(inclusion_rule = 'original_criteria', score_frailty, null)) as score_frailty_original_criteria
        , max(iff(inclusion_rule = 'adults_with_complexity', score_frailty, null)) as score_frailty_adults_with_complexity
    from {{ ref('cltcs_score_frailty') }}
    group by sk_patient_id
)

select il.patient_id
    , il.area_code
    , meets_original_criteria
    , meets_adults_with_complexity
    , a.score_activation
    , c.score_coordination
    , tr.score_treatment_original_criteria
    , tr.score_treatment_adults_with_complexity
    , ntr.score_treatment_nice_original_criteria
    , ntr.score_treatment_nice_adults_with_complexity
    , f.score_frailty_original_criteria
    , f.score_frailty_adults_with_complexity
from inclusion_list il
left join {{ ref('cltcs_score_activation') }} a on il.patient_id = a.sk_patient_id
left join {{ ref('cltcs_score_coordination') }} c on il.patient_id = c.sk_patient_id
left join treatment_by_route tr on il.patient_id = tr.sk_patient_id
left join nice_treatment_by_route ntr on il.patient_id = tr.sk_patient_id
left join frailty_by_route f on il.patient_id = f.sk_patient_id