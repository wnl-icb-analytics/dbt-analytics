{{
    config(
        materialized='table',
        cluster_by=['person_id'],
        tags=['smi_registry']
        )
}}

--SMI REGISTER BASE POPULATION HISTORICAL (using HAS_SMI flag) 
with eligible as (
select 
person_id, 
analysis_month, 
has_smi
FROM {{ ref('person_month_analysis_base') }} pmab
-- Limit to last 48 months (4 years)
WHERE analysis_month >= DATEADD('month', -48, CURRENT_DATE)
--AND IS_ACTIVE = TRUE
AND HAS_SMI
-- AND is_deceased = FALSE
)
SELECT
    pmab.person_id
    ,e.has_smi
    ,pmab.analysis_month    -- -- Fiscal year end flag
    ,pmab.financial_year as fiscal_year_label   
    ,pmab.age
    ,pmab.gender
    ,pmab.AGE_BAND_NHS
    ,CASE
        WHEN pmab.age_band_nhs = '5-14' THEN 1
        WHEN pmab.age_band_nhs = '15-24' THEN 2
        WHEN pmab.age_band_nhs = '25-34' THEN 3
        WHEN pmab.age_band_nhs = '35-44' THEN 4
        WHEN pmab.age_band_nhs = '45-54' THEN 5
        WHEN pmab.age_band_nhs = '55-64' THEN 6
        WHEN pmab.age_band_nhs = '65-74' THEN 7
        WHEN pmab.age_band_nhs = '75-84' THEN 8
        WHEN pmab.age_band_nhs = '85+' THEN 9
    END AS AGE_NHS_ORDER
    ,pmab.is_deceased
    ,pmab.is_active
    ,pmab.birth_date_approx
    -- Ethnicity
    ,pmab.ethnicity_category
    ,CASE
        WHEN pmab.ethnicity_category = 'Asian' THEN 1
        WHEN pmab.ethnicity_category = 'Black' THEN 2
        WHEN pmab.ethnicity_category = 'Mixed' THEN 3
        WHEN pmab.ethnicity_category = 'Other' THEN 4
        WHEN pmab.ethnicity_category = 'White' THEN 5
        WHEN pmab.ethnicity_category = 'Unknown' THEN 6
    END AS ethcat_order
    --ETHSUBCATEGORY
    ,CASE
        WHEN pmab.ETHNICITY_SUBCATEGORY in ('Not Recorded','Not stated','Not Stated','Recorded Not Known','Refused') THEN 'Unknown'
        ELSE pmab.ETHNICITY_SUBCATEGORY END AS ETHNICITY_SUBCATEGORY
    ,CASE 
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Asian: Bangladeshi' THEN 1
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Asian: Chinese' THEN 2
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Asian: Indian' THEN 3
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Asian: Pakistani' THEN 4
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Asian: Other Asian' THEN 5
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Black: African' THEN 6
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Black: Caribbean' THEN 7
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Black: Other Black' THEN 8
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Mixed: White and Asian' THEN 9
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Mixed: White and Black African' THEN 10
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Mixed: White and Black Caribbean' THEN 11
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Mixed: Other Mixed' THEN 12
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Other: Arab' THEN 13
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Other: Other' THEN 14
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'White: British' THEN 15
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'White: Irish' THEN 16
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'White: Traveller' THEN 17
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'White: Other White' THEN 18
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Unknown' THEN 19
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Not Recorded' THEN 19
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Not stated' THEN 19
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Not Stated' THEN 19
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Recorded Not Known' THEN 19
        WHEN pmab.ETHNICITY_SUBCATEGORY = 'Refused' THEN 19
        END AS ETHSUBCAT_ORDER
    -- IMD
    ,COALESCE(pmab.imd_quintile_25, 'Unknown') AS imd_quintile
    ,CASE
        WHEN pmab.imd_quintile_25 = 'Most Deprived' THEN 1
        WHEN pmab.imd_quintile_25 = 'Second Most Deprived' THEN 2
        WHEN pmab.imd_quintile_25 = 'Third Most Deprived' THEN 3
        WHEN pmab.imd_quintile_25 = 'Second Least Deprived' THEN 4
        WHEN pmab.imd_quintile_25 = 'Least Deprived' THEN 5
        ELSE 6
    END AS imdquintile_order
    -- Practice
    ,pmab.borough_registered as practice_borough
    ,COALESCE(pmab.neighbourhood_resident,'Unknown') as residential_neighbourhood
    ,pmab.pcn_name as primary_care_network
    ,pmab.practice_name
    ,pmab.practice_code
    ,CASE
    WHEN pmab.LOCAL_AUTHORITY_NAME not in ('Barnet','Camden','Enfield','Haringey','Islington') 
    THEN 'Outside NCL' 
    WHEN pmab.LOCAL_AUTHORITY_NAME IS NULL THEN 'Unknown'
    ELSE pmab.LOCAL_AUTHORITY_NAME END as RESIDENTIAL_BOROUGH
    ,CASE
    WHEN pmab.LOCAL_AUTHORITY_NAME not in ('Barnet','Camden','Enfield','Haringey','Islington') 
    THEN 'Outside NCL'
    WHEN pmab.LOCAL_AUTHORITY_NAME IS NULL THEN 'Unknown'
    ELSE pmab.ward_name END as WARD_NAME
FROM {{ ref('person_month_analysis_base') }} pmab
--FROM REPORTING.OLIDS_PERSON_ANALYTICS.PERSON_MONTH_ANALYSIS_BASE  pmab
INNER JOIN eligible e
    ON pmab.person_id = e.person_id
    AND pmab.analysis_month = e.analysis_month
    

