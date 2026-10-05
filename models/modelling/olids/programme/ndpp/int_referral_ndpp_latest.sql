{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--using PCDREFSET CODES FOR NDPP REFERRALS including invitations sent and declines - Selecting latest per person
SELECT
    person_id,
    clinical_effective_date,
    concept_code,
    concept_display
    FROM {{ ref('int_referral_ndpp_all') }}
QUALIFY
    ROW_NUMBER()
        OVER (
            PARTITION BY person_id
            ORDER BY clinical_effective_date DESC,
                -- Preserve the existing SMI latest-event priority.
                CASE concept_code
                    WHEN '1025301000000100' THEN 1
                    WHEN '1090701000000104' THEN 0
                    ELSE -1
                END DESC,
                concept_code
        )
    = 1
