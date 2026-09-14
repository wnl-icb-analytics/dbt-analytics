/*
Simplified Carer Eligibility Rule

Business Rule: Person is eligible if they have:
1. Latest carer code (CARER_COD) AND no more recent not-carer code (NOTCARER_COD)
   - CARER_COD > NOTCARER_COD (or no NOTCARER_COD at all)
2. AND NOT a member of ATRISK_GROUP (the 8 clinical risk groups), BMI_GROUP or
   PREG_GROUP. Learning disability is not an at-risk group, so it does not exclude.
3. AND aged 5 years or older (minimum age for carer flu vaccination)

Exclusion rule - carer status with exclusion from other eligibility routes.
*/

{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='campaign_id',
    tags=['covid_flu']
) }}

WITH all_campaigns AS (
    -- Every flu campaign the models report on
    -- (campaign list: macros/config/flu_campaign_selection.sql)
    {{ flu_build_campaigns() }}
),

-- Step 1: Find people with carer codes (for all campaigns)
people_with_carer_codes AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_carer_date,
        cc.audit_end_date
    FROM ({{ get_observations("'CARER_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 2: Find people with not-carer codes (for all campaigns)
people_with_not_carer_codes AS (
    SELECT 
        cc.campaign_id,
        obs.person_id,
        MAX(obs.clinical_effective_date) AS latest_not_carer_date,
        cc.audit_end_date
    FROM ({{ get_observations("'NOTCARER_COD'", 'UKHSA_FLU', versioned=true) }}) obs
    CROSS JOIN all_campaigns cc
    WHERE obs.spec_version = cc.terminology_version
        AND obs.clinical_effective_date IS NOT NULL
        AND obs.clinical_effective_date <= cc.audit_end_date
    GROUP BY cc.campaign_id, obs.person_id, cc.audit_end_date
),

-- Step 3: Apply carer status logic (carer more recent than not-carer, for all campaigns)
people_with_active_carer_status AS (
    SELECT 
        cc.campaign_id,
        cc.person_id,
        cc.latest_carer_date,
        'Active carer status (not superseded by not-carer code)' AS eligibility_reason,
        'Carer: ' || cc.latest_carer_date || 
        ', Not-carer: ' || COALESCE(ncc.latest_not_carer_date::VARCHAR, 'none') AS status_comparison,
        cc.audit_end_date
    FROM people_with_carer_codes cc
    LEFT JOIN people_with_not_carer_codes ncc
        ON cc.campaign_id = ncc.campaign_id
        AND cc.person_id = ncc.person_id
    WHERE cc.latest_carer_date > COALESCE(ncc.latest_not_carer_date, '1900-01-01'::DATE)
),

-- Step 4: Identify people eligible via other routes (exclusions, for all campaigns)
-- Only ATRISK_GROUP, BMI_GROUP and PREG_GROUP exclude a carer. Learning disability is
-- not part of ATRISK_GROUP and so is deliberately absent here.
people_eligible_via_other_routes AS (
    -- Clinical risk groups (ATRISK_GROUP)
    SELECT DISTINCT campaign_id, person_id, 'clinical_risk_group' AS exclusion_reason
    FROM (
        SELECT campaign_id, person_id FROM {{ ref('int_flu_asthma') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_chronic_heart_disease') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_chronic_kidney_disease') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_chronic_liver_disease') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_diabetes') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_immunosuppression') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_chronic_neurological_disease') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_asplenia') }}
        UNION SELECT campaign_id, person_id FROM {{ ref('int_flu_chronic_respiratory_disease') }}
    )
    
    UNION ALL
    
    -- BMI group
    SELECT DISTINCT campaign_id, person_id, 'bmi_group' AS exclusion_reason
    FROM {{ ref('int_flu_severe_obesity') }}
    
    UNION ALL
    
    -- Pregnancy group
    SELECT DISTINCT campaign_id, person_id, 'pregnancy_group' AS exclusion_reason
    FROM {{ ref('int_flu_pregnancy') }}
),

-- Step 5: Apply exclusion logic - remove people eligible via other routes (for all campaigns)
people_eligible_as_carers_only AS (
    SELECT 
        pacs.campaign_id,
        pacs.person_id,
        pacs.latest_carer_date,
        pacs.eligibility_reason,
        pacs.status_comparison,
        pacs.audit_end_date
    FROM people_with_active_carer_status pacs
    WHERE NOT EXISTS (
        SELECT 1 
        FROM people_eligible_via_other_routes pevor
        WHERE pevor.campaign_id = pacs.campaign_id
        AND pevor.person_id = pacs.person_id
    )
),

-- Step 6: Add demographics and apply age restrictions (for all campaigns)
final_eligibility AS (
    SELECT 
        peco.campaign_id,
        'Clinical Condition' AS campaign_category,
        'Carer' AS risk_group,
        peco.person_id,
        peco.latest_carer_date AS qualifying_event_date,
        cc.campaign_reference_date AS reference_date,
        'Carers aged 5 to under 65, not eligible via other risk groups' AS description,
        demo.birth_date_approx,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx)) AS age_months_at_ref_date,
        FLOOR(MONTHS_BETWEEN(cc.campaign_reference_date, demo.birth_date_approx) / 12) AS age_years_at_ref_date,
        peco.audit_end_date AS created_at
    FROM people_eligible_as_carers_only peco
    JOIN all_campaigns cc
        ON peco.campaign_id = cc.campaign_id
    JOIN {{ ref('dim_person_demographics') }} demo
        ON peco.person_id = demo.person_id
    WHERE 1=1
        -- Aged 5 or over at RUN_DAT and under 65 at REF_DAT (spec indicator 20, age bands
        -- 4 to 6). Carers aged 65 and over are reported by the 65 and over indicator, and
        -- the spec has no carer field for them. Tested on birth date for the upper bound,
        -- because DATEDIFF subtracts calendar parts rather than counting completed years.
        AND DATEADD('year', 5, demo.birth_date_approx) <= cc.run_date
        AND demo.birth_date_approx > DATEADD('year', -65, cc.campaign_reference_date)
)

SELECT * FROM final_eligibility
ORDER BY person_id
