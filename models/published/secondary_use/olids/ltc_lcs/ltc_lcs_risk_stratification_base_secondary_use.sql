{{
    config(
        materialized='table',
        alias='ltc_lcs_risk_stratification_base',
        cluster_by=['overall_risk_rank'],
        meta={
            'custom_message': 'Includes OLIDS data published for secondary use under a Section 251 exemption as a HealtheIntent replacement dashboard. National Data Opt-Out and Type 1 opt-out filtering is intentionally NOT applied. Approved for this s251 purpose only.'
        }
    )
}}

-- LTC LCS Risk Stratification Base Table - Secondary Use (Section 251 exempt)
-- Secondary use variant published under a Section 251 exemption as a HealtheIntent
-- replacement dashboard. Opt-out filtering is intentionally NOT applied - this is a
-- passthrough of the direct care base for the approved s251 purpose.
-- See ltc_lcs_risk_stratification_base in published_reporting_direct_care for full documentation.

select *
from {{ ref('ltc_lcs_risk_stratification_base') }}
