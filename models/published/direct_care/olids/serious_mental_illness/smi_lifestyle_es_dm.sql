{{
    config(
        materialized='view',
        tags=['smi_registry']
    )
}}

-- Preserve published text widths when Fusion narrows the upstream CASE results.
select * replace (
    referral_diet_advice::varchar as referral_diet_advice,
    referral_exercise_advice::varchar as referral_exercise_advice
)
FROM {{ ref('int_smi_lifestyle_es') }}
