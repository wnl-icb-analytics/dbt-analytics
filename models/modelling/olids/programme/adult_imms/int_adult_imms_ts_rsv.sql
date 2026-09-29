{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}

--aggregated output one dose 2 cohorts. Aggregate 2.8 million rows to 350K rows
select 
p.vaccine
,p.cohort
,p.analysis_month
,p.fiscal_year_label
,CASE
WHEN p.cohort ='AGE 75+' THEN 7
WHEN p.cohort ='CLINICAL RISK 65-74' THEN 8
END As VACC_ORDER 
,p.practice_code
,p.age_band_5y
,p.ethnicity_category
,CASE
        WHEN p.ethnicity_category = 'Asian' THEN 1
        WHEN p.ethnicity_category = 'Black' THEN 2
        WHEN p.ethnicity_category = 'Mixed' THEN 3
        WHEN p.ethnicity_category = 'Other' THEN 4
        WHEN p.ethnicity_category = 'White' THEN 5
        WHEN p.ethnicity_category = 'Unknown' THEN 6
    END AS ethcat_order
,p.imd_quintile
,CASE
        WHEN p.imd_quintile = 'Most Deprived' THEN 1
        WHEN p.imd_quintile = 'Second Most Deprived' THEN 2
        WHEN p.imd_quintile = 'Third Most Deprived' THEN 3
        WHEN p.imd_quintile = 'Second Least Deprived' THEN 4
        WHEN p.imd_quintile = 'Least Deprived' THEN 5
        ELSE 6
    END AS imdquintile_order
,p.numerator 
,p.denominator 
FROM (
--Age 75+ 
select 
vaccine, cohort, analysis_month, fiscal_year_label, practice_code, age_band_5y, ethnicity_category, imd_quintile, 
sum(vaccinated) as numerator, count(*) as denominator 
FROM {{ ref('int_adult_imms_ts_historical_rsv') }}
where cohort = 'AGE 75+'
group by all

UNION
--CLINICAL RISK 65-74
select 
vaccine, cohort, analysis_month, fiscal_year_label, practice_code, age_band_5y, ethnicity_category, imd_quintile,  
sum(vaccinated) as numerator, count(*) as denominator 
FROM {{ ref('int_adult_imms_ts_historical_rsv') }}
where cohort = 'CLINICAL RISK 65-74'
group by all
) p

