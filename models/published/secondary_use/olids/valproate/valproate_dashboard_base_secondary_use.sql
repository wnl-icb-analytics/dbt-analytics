{{
    config(
        materialized='table',
        alias='valproate_dashboard_base',
        meta={
            'custom_message': 'Includes OLIDS data published for secondary use under a Section 251 exemption as a HealtheIntent replacement dashboard. National Data Opt-Out and Type 1 opt-out filtering is intentionally NOT applied. Approved for this s251 purpose only.'
        }
    )
}}

/*
Valproate Dashboard Base Table - Secondary Use (Section 251 exempt)

Secondary use variant published under a Section 251 exemption as a HealtheIntent
replacement dashboard. Opt-out filtering is intentionally NOT applied - this is a
passthrough of the direct care base for the approved s251 purpose. The base retains
the IS_OPTED_OUT display flag for clinical context but does not filter on it.

See view_valproate_dashboard_base in published_reporting_direct_care for full documentation.
*/

SELECT *
FROM {{ ref('view_valproate_dashboard_base') }}
