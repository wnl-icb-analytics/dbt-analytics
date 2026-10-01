{{
    config(
        materialized='table',
        cluster_by=['sk_patient_id'],
        tags=['asc_service']
    )
}}

select
    sk_patient_id,
    1 as has_asc_service,
    array_agg(distinct borough_name) as borough_name,
    array_agg(distinct reporting_period_start_date) as reporting_period_start_date,
    array_agg(distinct reporting_period_end_date) as reporting_period_end_date,
    array_agg(distinct service_type) as service_type,
    array_agg(distinct primary_support_reason_category) as primary_support_reason_category,
    array_size(array_agg(distinct primary_support_reason_category) ) as primary_support_reason_category_count,
    boolor_agg(
        primary_support_reason_cleaned = 'Physical support: Personal care support'
    ) as has_physical_support_personal_care,
    boolor_agg(
        primary_support_reason_cleaned = 'Physical support: Access and mobility only'
    ) as has_physical_support_access_mobility,
    boolor_agg(
        primary_support_reason_cleaned = 'Learning disability support'
    ) as has_learning_disability_support,
    boolor_agg(
        primary_support_reason_cleaned = 'Mental health support'
    ) as has_mental_health_support,
    boolor_agg(
        primary_support_reason_cleaned = 'Unknown'
    ) as has_unknown_primary_support_reason,
    boolor_agg(
        primary_support_reason_cleaned = 'Support with memory and cognition'
    ) as has_memory_cognition_support,
    boolor_agg(
        primary_support_reason_cleaned = 'Social support: Support to unpaid carer'
    ) as has_social_support_unpaid_carer,
    boolor_agg(
        primary_support_reason_cleaned = 'Social support: Support for social isolation or other'
    ) as has_social_support_social_isolation,
    boolor_agg(
        primary_support_reason_cleaned = 'Sensory support: Support for visual impairment'
    ) as has_sensory_support_visual_impairment,
    boolor_agg(
        primary_support_reason_cleaned = 'Sensory support: Support for hearing impairment'
    ) as has_sensory_support_hearing_impairment,
    boolor_agg(
        primary_support_reason_cleaned = 'Social support: Substance misuse support'
    ) as has_social_support_substance_misuse,
    boolor_agg(
        primary_support_reason_cleaned = 'Sensory support: Support for dual impairment'
    ) as has_sensory_support_dual_impairment,
    boolor_agg(
        primary_support_reason_cleaned = 'Social support: Asylum seeker support'
    ) as has_social_support_asylum_seeker
from {{ref('int_asc_cld_service_most_recent')}}
group by sk_patient_id