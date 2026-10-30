{{
    config(
        materialized='table',
        cluster_by=['person_id'])
}}

/*
Latest NHS Health Check completion per person.
Used for health check programme analysis and eligibility assessment.
*/

SELECT
    person_id,
    ID,
    clinical_effective_date,
    concept_code,
    concept_display,
    is_completed_health_check,
    health_check_current_12m,
    health_check_current_24m,
    health_check_current_5y

FROM (
    {{ get_latest_events(
        ref('int_nhs_health_check_all'),
        partition_by=['person_id'],
        order_by=['clinical_effective_date::DATE', 'date_recorded DESC NULLS LAST', 'id']
    ) }}
) latest_health_check
