{{
    config(
        materialized='table',
        tags=['screening_programme']
        )
}}
/*
All breast screening programme observations from clinical records.
Uses PCD REFSET breast screening cluster IDs:
- BRCANSCR_COD: Breast screening completed codes
- BRCANSCRDEC_COD: Breast screening declined codes

Clinical Purpose:
- Breast screening programme data collection
- Observation-level screening events tracking
- Foundation data for programme analysis

Key Business Rules:
- Females aged 50 to 70 : invited every 3 years
- Declined/non-response status: valid for 12 months only - NON RESPONSE NOT AVAIALBLE IN PCD REFSETS
- Unsuitable status: permanent unless superseded by completed screening - NOT AVAIALBLE IN PCD REFSETS

Includes ALL persons (active, inactive, deceased) following intermediate layer principles.
This is OBSERVATION-LEVEL data - one row per breast screening observation.
2026-08-05 Rachel spotted an error - range should be 50-70 not 50-71. Updated to reflect this.
*/
WITH person_demographics AS (
    SELECT
        dpa.person_id,
        CASE WHEN dpa.gender = 'Female' THEN TRUE ELSE FALSE END AS is_female,
        dpa.age AS current_age,
        dpa.is_active,
        dpa.is_deceased,
        -- Age-based screening eligibility
        CASE
            WHEN dpa.gender != 'Female' THEN FALSE
            WHEN dpa.age BETWEEN 50 AND 70 THEN TRUE
            ELSE FALSE
        END AS is_screening_eligible,
        
        -- Screening interval
        3 AS screening_interval_years,
        
        -- Target screening frequency in days
        1095  AS screening_interval_days
        
    FROM {{ ref('dim_person_demographics') }} dpa
    --FROM REPORTING.OLIDS_PERSON_DEMOGRAPHICS.DIM_PERSON_DEMOGRAPHICS dpa
        WHERE dpa.gender = 'Female'  -- Only include women
        AND dpa.age BETWEEN 50 AND 70  -- Only include eligible age range
),

screening_history AS (
    SELECT
        person_id,
        
        -- Latest dates by type
        MAX(CASE WHEN is_completed_screening THEN clinical_effective_date END) AS latest_completed_date,
       -- MAX(CASE WHEN is_unsuitable_screening THEN clinical_effective_date END) AS latest_unsuitable_date,
        MAX(CASE WHEN is_declined_screening THEN clinical_effective_date END) AS latest_declined_date,
       -- MAX(CASE WHEN is_non_response_screening THEN clinical_effective_date END) AS latest_non_response_date,
        
        -- Counts by type
        COUNT(CASE WHEN is_completed_screening THEN 1 END) AS total_completed_screenings,
       -- COUNT(CASE WHEN is_unsuitable_screening THEN 1 END) AS total_unsuitable_records,
        COUNT(CASE WHEN is_declined_screening THEN 1 END) AS total_declined_records,
       -- COUNT(CASE WHEN is_non_response_screening THEN 1 END) AS total_non_response_records,
        
        -- Overall screening history
        MIN(clinical_effective_date) AS earliest_screening_date,
        MAX(clinical_effective_date) AS latest_screening_date,
        COUNT(*) AS total_screening_observations
        
    FROM {{ ref('int_breast_screening_all') }}
    -- FROM DEV__MODELLING.OLIDS_PROGRAMME.INT_BREAST_SCREENING_ALL
    GROUP BY person_id
),

programme_status AS (
    SELECT
        pd.person_id,
        pd.current_age,
        pd.is_active,
        pd.is_deceased,
        pd.is_screening_eligible,
        pd.screening_interval_years,
        pd.screening_interval_days,
        
        -- Latest screening information
        cls.clinical_effective_date AS latest_screening_date,
        cls.source_cluster_id AS latest_screening_type,
        cls.screening_observation_type,
        
        -- Core screening flags
        cls.is_completed_screening AS latest_is_completed,
       -- cls.is_unsuitable_screening AS latest_is_unsuitable,
        cls.is_declined_screening AS latest_is_declined,
        --cls.is_non_response_screening AS latest_is_non_response,
        
        -- Screening history
        sh.latest_completed_date,
        sh.total_completed_screenings,
       -- sh.total_unsuitable_records,
        sh.total_declined_records,
       -- sh.total_non_response_records,
        
        -- Never screened flag
        CASE
            WHEN sh.total_completed_screenings = 0 OR sh.total_completed_screenings IS NULL 
            THEN TRUE
            ELSE FALSE
        END AS never_screened,
        
        -- Programme compliance status (simplified)
        CASE
            WHEN sh.total_completed_screenings = 0 OR sh.latest_completed_date IS NULL THEN 'Never Screened'
            -- WHEN sh.latest_unsuitable_date IS NOT NULL 
            --     AND (sh.latest_completed_date IS NULL OR sh.latest_unsuitable_date > sh.latest_completed_date)
            --     THEN 'Unsuitable'
            WHEN DATEDIFF(day, sh.latest_completed_date, CURRENT_DATE()) <= pd.screening_interval_days THEN 'Up to Date'
            WHEN DATEDIFF(day, sh.latest_completed_date, CURRENT_DATE()) > pd.screening_interval_days THEN 'Overdue'
            ELSE 'Unknown'
        END AS programme_status,
        
        -- Next screening due calculation
        CASE
            WHEN sh.latest_completed_date IS NOT NULL AND pd.screening_interval_days IS NOT NULL
            THEN DATEADD(day, pd.screening_interval_days, sh.latest_completed_date)
            ELSE NULL
        END AS next_screening_due_date,
        
        -- Days overdue calculation
        CASE
            WHEN sh.latest_completed_date IS NOT NULL AND pd.screening_interval_days IS NOT NULL
                AND DATEDIFF(day, sh.latest_completed_date, CURRENT_DATE()) > pd.screening_interval_days
            THEN DATEDIFF(day, sh.latest_completed_date, CURRENT_DATE()) - pd.screening_interval_days
            ELSE NULL
        END AS days_overdue
        
    FROM person_demographics pd
    LEFT JOIN {{ ref('int_breast_screening_latest') }} cls ON pd.person_id = cls.person_id
    LEFT JOIN screening_history sh ON pd.person_id = sh.person_id
)

SELECT
    person_id,
    current_age,
    is_active,
    is_deceased,
    is_screening_eligible,
    screening_interval_years,
    
    -- Programme status
    programme_status,
    screening_observation_type AS latest_screening_type,
    
    -- Key dates
    latest_screening_date,
    latest_completed_date,
    next_screening_due_date,
    days_overdue,
    
    -- Core flags
    never_screened,
    latest_is_completed,
   -- latest_is_unsuitable,
    latest_is_declined,
   -- latest_is_non_response,
    
    -- Screening history counts
    total_completed_screenings,
   -- total_unsuitable_records,
    total_declined_records,
   -- total_non_response_records

FROM programme_status
-- Already filtered to eligible women (50 - 70) in person_demographics CTE
ORDER BY person_id