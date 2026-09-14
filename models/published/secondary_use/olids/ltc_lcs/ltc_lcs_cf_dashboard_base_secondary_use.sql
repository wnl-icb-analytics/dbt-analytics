{{
    config(
        materialized='table',
        alias='ltc_lcs_cf_dashboard_base',
        cluster_by=['indicator_category', 'indicator_id'],
        meta={
            'custom_message': 'Includes OLIDS data published for secondary use under a Section 251 exemption as a HealtheIntent replacement dashboard. National Data Opt-Out and Type 1 opt-out filtering is intentionally NOT applied. Approved for this s251 purpose only.'
        }
    )
}}

/*
LTC/LCS Case Finding Dashboard Base Table - Secondary Use (Section 251 exempt)

Secondary use variant published under a Section 251 exemption as a HealtheIntent
replacement dashboard. Opt-out filtering is intentionally NOT applied - this is a
passthrough of the direct care base for the approved s251 purpose.

See ltc_lcs_cf_dashboard_base in published_reporting_direct_care for full documentation.
*/

SELECT *
FROM {{ ref('ltc_lcs_cf_dashboard_base') }}
