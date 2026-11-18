{{
    config(
        materialized='table',
        tags=['screening_programme']
        )
}}

/*
Cervical Screening Programme Status - Person-Level Analytics
Simplified tracking focused on core programme monitoring requirements.

Key Business Rules:
1. Screening Intervals:
   - QOF achievement windows: 42 months at 25-49, 66 months at 50-64. These are not
     recall dates; routine recall after an HPV-negative test is 5 years from July 2025
   - Women outside 25-64 age range: not eligible for routine screening

2. Programme Status, in QOF v51 CS005/CS006 rule order:
   - Cervix Removed: any complete cervix removal record (NOCX_COD)
   - Up to Date: completed screening within the interval (42 months to 49, 66 months from 50)
   - Unsuitable: unsuitability record (CSPU_COD) within the interval
   - Pregnant: currently pregnant, per fct_person_pregnancy_status (all ages; QOF applies
     pregnancy to CS005 only)
   - Never Screened: no completed screening history
   - Overdue: last completed screening outside the interval

Clinical Purpose:
- Programme compliance monitoring
- Screening interval tracking and invitation planning
- Population coverage reporting
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
            WHEN dpa.age BETWEEN 25 AND 64 THEN TRUE
            ELSE FALSE
        END AS is_screening_eligible,
        
        -- Age-specific screening interval
        -- QOF v51 intervals (unchanged from v50)
        CASE
           -- WHEN dpa.age BETWEEN 25 AND 49 THEN 3  -- 3-year interval
            WHEN dpa.age BETWEEN 25 AND 49 THEN 3.5 -- 3.5-year interval (QOF)
            --WHEN dpa.age BETWEEN 50 AND 64 THEN 5  -- 5-year interval
            WHEN dpa.age BETWEEN 50 AND 64 THEN 5.5  -- 5.5-year interval (QOF)
            ELSE NULL
        END AS screening_interval_years,
        
        -- QOF v51 CS005/CS006 windows
        CASE
            WHEN dpa.age BETWEEN 25 AND 49 THEN 42
            WHEN dpa.age BETWEEN 50 AND 64 THEN 66
        END AS screening_interval_months
        
    FROM {{ ref('dim_person_demographics') }} dpa
    WHERE dpa.gender = 'Female'  -- Only include women
        AND dpa.age BETWEEN 25 AND 64  -- Only include eligible age range
),

screening_history AS (
    SELECT
        person_id,
        
        -- Latest dates by type
        MAX(CASE WHEN is_completed_screening THEN clinical_effective_date END) AS latest_completed_date,
        MAX(CASE WHEN is_unsuitable_screening THEN clinical_effective_date END) AS latest_unsuitable_date,
        MAX(CASE WHEN is_declined_screening THEN clinical_effective_date END) AS latest_declined_date,
        MAX(CASE WHEN is_non_response_screening THEN clinical_effective_date END) AS latest_non_response_date,
        
        -- Counts by type
        COUNT(DISTINCT IFF(is_completed_screening, {{ observation_reading_key() }}, NULL)) AS total_completed_screenings,
        COUNT(DISTINCT IFF(is_unsuitable_screening, {{ observation_reading_key() }}, NULL)) AS total_unsuitable_records,
        COUNT(DISTINCT IFF(is_declined_screening, {{ observation_reading_key() }}, NULL)) AS total_declined_records,
        COUNT(DISTINCT IFF(is_non_response_screening, {{ observation_reading_key() }}, NULL)) AS total_non_response_records,
        MIN(CASE WHEN is_cervix_removal THEN clinical_effective_date END) AS first_cervix_removal_date,
        
        -- Overall screening history
        MIN(clinical_effective_date) AS earliest_screening_date,
        MAX(clinical_effective_date) AS latest_screening_date,
        COUNT(DISTINCT {{ observation_reading_key() }}) AS total_screening_observations
        
    FROM {{ ref('int_cervical_screening_all') }}
    GROUP BY person_id
),

programme_status AS (
    SELECT
        pd.person_id,
        pd.current_age,
        pd.is_active,
        pd.is_deceased,
        pd.is_screening_eligible AND sh.first_cervix_removal_date IS NULL AS is_screening_eligible,
        pd.screening_interval_years,
        
        -- Latest screening information
        cls.clinical_effective_date AS latest_screening_date,
        cls.source_cluster_id AS latest_screening_type,
        cls.screening_observation_type,
        
        -- Core screening flags
        cls.is_completed_screening AS latest_is_completed,
        cls.is_unsuitable_screening AS latest_is_unsuitable,
        cls.is_declined_screening AS latest_is_declined,
        cls.is_non_response_screening AS latest_is_non_response,
        
        -- Screening history
        sh.latest_completed_date,
        sh.latest_unsuitable_date,
        sh.first_cervix_removal_date,
        preg.person_id IS NOT NULL AS is_currently_pregnant,
        sh.total_completed_screenings,
        sh.total_unsuitable_records,
        sh.total_declined_records,
        sh.total_non_response_records,
        
        -- Never screened flag
        CASE
            WHEN sh.total_completed_screenings = 0 OR sh.total_completed_screenings IS NULL 
            THEN TRUE
            ELSE FALSE
        END AS never_screened,
        
        -- Programme compliance status
        CASE
            WHEN sh.first_cervix_removal_date IS NOT NULL THEN 'Cervix Removed'
            WHEN sh.latest_completed_date::DATE > DATEADD(month, -pd.screening_interval_months, CURRENT_DATE()) THEN 'Up to Date'
            WHEN sh.latest_unsuitable_date::DATE > DATEADD(month, -pd.screening_interval_months, CURRENT_DATE()) THEN 'Unsuitable'
            WHEN preg.person_id IS NOT NULL THEN 'Pregnant'
            WHEN sh.latest_completed_date IS NULL THEN 'Never Screened'
            ELSE 'Overdue'
        END AS programme_status,

        -- End of the QOF achievement window; null without a cervix
        CASE
            WHEN sh.first_cervix_removal_date IS NULL
            THEN DATEADD(month, pd.screening_interval_months, sh.latest_completed_date)
        END AS next_screening_due_date

    FROM person_demographics pd
    LEFT JOIN {{ ref('int_cervical_screening_latest') }} cls ON pd.person_id = cls.person_id
    LEFT JOIN screening_history sh ON pd.person_id = sh.person_id
    LEFT JOIN {{ ref('fct_person_pregnancy_status') }} preg ON pd.person_id = preg.person_id
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
    CASE
        WHEN programme_status = 'Overdue'
        THEN DATEDIFF(day, next_screening_due_date, CURRENT_DATE())
    END AS days_overdue,
    latest_unsuitable_date,
    first_cervix_removal_date,
    is_currently_pregnant,
    
    -- Core flags
    never_screened,
    latest_is_completed,
    latest_is_unsuitable,
    latest_is_declined,
    latest_is_non_response,
    
    -- Screening history counts
    total_completed_screenings,
    total_unsuitable_records,
    total_declined_records,
    total_non_response_records

FROM programme_status
-- Already filtered to eligible women (25-64) in person_demographics CTE

ORDER BY person_id
