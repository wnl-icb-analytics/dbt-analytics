{{
    config(
        materialized='table',
        cluster_by=['person_id', 'clinical_effective_date'],
        tags=['smi_registry']
        )
}}
--using mixed codes defined by SMI Longer Lives Campaign Alcohol Misuse education and referral to treatment. Added 2 extra codes from PCDrefset
select *
FROM {{ ref('int_alcohol_intervention') }}
QUALIFY ROW_NUMBER() OVER (PARTITION BY person_id ORDER BY clinical_effective_date DESC,
    CASE alcohol_advice_services WHEN 'Yes' THEN 1 WHEN 'Declined' THEN 2 ELSE 3 END,
    concept_code) = 1