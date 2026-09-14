/*
Clinical risk group membership per programme, campaign and person, at any age.

The eligibility facts publish clinical groups only where they drive the offer: under 65
for the at-risk cohort, under 75 for COVID immunosuppression. The UKHSA uptake
indicators report each clinical group across every age band, including 65 and over
(flu bands 1 to 6, COVID bands 17 to 20), so a 70 year old with CKD needs a CKD flag on
their "Age 65 and Over" row.

This model reads the clinical intermediates directly, which compute every campaign and
every age, and pivots membership to one row per programme, campaign and person. The
dashboards join it on those three keys, so the flags are bounded to the campaign's own
evidence window and do not leak between seasons or programmes.

in_clinical_risk_group is the spec ATRISK_GROUP: the eight shared clinical groups, plus
learning disability and severe mental illness for COVID only. Flu treats learning
disability as a reporting group, not an at-risk group.
*/

{{ config(
    materialized='table',
    tags=['covid_flu'],
    cluster_by=['programme_type', 'campaign_id', 'person_id']
) }}

WITH memberships AS (
    SELECT 'COVID' AS programme_type, campaign_id, person_id, 'asthma' AS risk_flag FROM {{ ref('int_covid_asthma') }}
    UNION ALL
    SELECT 'COVID', campaign_id, person_id, 'asplenia' FROM {{ ref('int_covid_asplenia') }}
    UNION ALL
    SELECT 'COVID', campaign_id, person_id, 'chd' FROM {{ ref('int_covid_chronic_heart_disease') }}
    UNION ALL
    SELECT 'COVID', campaign_id, person_id, 'ckd' FROM {{ ref('int_covid_chronic_kidney_disease') }}
    UNION ALL
    SELECT 'COVID', campaign_id, person_id, 'cld' FROM {{ ref('int_covid_chronic_liver_disease') }}
    UNION ALL
    SELECT 'COVID', campaign_id, person_id, 'cnd' FROM {{ ref('int_covid_chronic_neurological_disease') }}
    UNION ALL
    SELECT 'COVID', campaign_id, person_id, 'crd' FROM {{ ref('int_covid_chronic_respiratory_disease') }}
    UNION ALL
    SELECT 'COVID', campaign_id, person_id, 'diabetes' FROM {{ ref('int_covid_diabetes') }}
    UNION ALL
    SELECT 'COVID', campaign_id, person_id, 'immunosuppression' FROM {{ ref('int_covid_immunosuppression') }}
    UNION ALL
    SELECT 'COVID', campaign_id, person_id, 'ld' FROM {{ ref('int_covid_learning_disability') }}
    UNION ALL
    SELECT 'COVID', campaign_id, person_id, 'smi' FROM {{ ref('int_covid_severe_mental_illness') }}

    UNION ALL

    SELECT 'FLU', campaign_id, person_id, 'asthma' FROM {{ ref('int_flu_asthma') }}
    UNION ALL
    SELECT 'FLU', campaign_id, person_id, 'asplenia' FROM {{ ref('int_flu_asplenia') }}
    UNION ALL
    SELECT 'FLU', campaign_id, person_id, 'chd' FROM {{ ref('int_flu_chronic_heart_disease') }}
    UNION ALL
    SELECT 'FLU', campaign_id, person_id, 'ckd' FROM {{ ref('int_flu_chronic_kidney_disease') }}
    UNION ALL
    SELECT 'FLU', campaign_id, person_id, 'cld' FROM {{ ref('int_flu_chronic_liver_disease') }}
    UNION ALL
    SELECT 'FLU', campaign_id, person_id, 'cnd' FROM {{ ref('int_flu_chronic_neurological_disease') }}
    UNION ALL
    SELECT 'FLU', campaign_id, person_id, 'crd' FROM {{ ref('int_flu_chronic_respiratory_disease') }}
    UNION ALL
    SELECT 'FLU', campaign_id, person_id, 'diabetes' FROM {{ ref('int_flu_diabetes') }}
    UNION ALL
    SELECT 'FLU', campaign_id, person_id, 'immunosuppression' FROM {{ ref('int_flu_immunosuppression') }}
    UNION ALL
    SELECT 'FLU', campaign_id, person_id, 'ld' FROM {{ ref('int_flu_learning_disability') }}
)

SELECT
    programme_type,
    campaign_id,
    person_id,
    MAX(IFF(risk_flag = 'asthma', 1, 0)) AS has_asthma,
    MAX(IFF(risk_flag = 'asplenia', 1, 0)) AS has_asplenia,
    MAX(IFF(risk_flag = 'chd', 1, 0)) AS has_chd,
    MAX(IFF(risk_flag = 'ckd', 1, 0)) AS has_ckd,
    MAX(IFF(risk_flag = 'cld', 1, 0)) AS has_cld,
    MAX(IFF(risk_flag = 'cnd', 1, 0)) AS has_cnd,
    MAX(IFF(risk_flag = 'crd', 1, 0)) AS has_crd,
    MAX(IFF(risk_flag = 'diabetes', 1, 0)) AS has_diabetes,
    MAX(IFF(risk_flag = 'immunosuppression', 1, 0)) AS is_immunosuppressed,
    MAX(IFF(risk_flag = 'ld', 1, 0)) AS has_ld,
    MAX(IFF(risk_flag = 'smi', 1, 0)) AS has_smi,
    -- Spec ATRISK_GROUP. Learning disability and SMI count for COVID only.
    MAX(IFF(
        risk_flag IN ('asthma', 'asplenia', 'chd', 'ckd', 'cld', 'cnd', 'crd', 'diabetes', 'immunosuppression')
        OR (programme_type = 'COVID' AND risk_flag IN ('ld', 'smi')),
        1, 0
    )) AS in_clinical_risk_group
FROM memberships
GROUP BY programme_type, campaign_id, person_id
