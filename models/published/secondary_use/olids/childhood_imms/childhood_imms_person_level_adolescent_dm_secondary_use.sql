{{
    config(
        materialized='table',
        alias='childhood_imms_person_level_adolescent_dm',
        meta={
            'custom_message': 'Includes OLIDS data published for secondary use under a Section 251 exemption as a HealtheIntent replacement dashboard. National Data Opt-Out and Type 1 opt-out filtering is intentionally NOT applied. Approved for this s251 purpose only.'
        }
    )
}}

/*
Childhood Immunisations Person Level Adolescent - Secondary Use (Section 251 exempt)

Secondary use variant published under a Section 251 exemption as a HealtheIntent
replacement dashboard. Opt-out filtering is intentionally NOT applied - this is a
passthrough of the direct care base for the approved s251 purpose.

See childhood_imms_person_level_adolescent_dm in published_reporting_direct_care for full documentation.
*/

SELECT *
FROM {{ ref('childhood_imms_person_level_adolescent_dm') }}
