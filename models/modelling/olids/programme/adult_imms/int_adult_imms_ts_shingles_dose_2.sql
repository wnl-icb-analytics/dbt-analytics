{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}
--Aggregate 2.5 million rows to 233K rows
--aggregated output first shingles dose 2 cohorts
select 
p.vaccine
,p.cohort
,p.analysis_month
,p.fiscal_year_label
,CASE
WHEN p.cohort ='TURN_65_AFTER_SEP_2023' THEN 5
WHEN p.cohort = '70-79 CATCH UP' THEN 6
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
--TURN_65_AFTER_SEP_2023 
select 
vaccine, cohort, analysis_month, fiscal_year_label, practice_code, age_band_5y, ethnicity_category,  imd_quintile, 
sum(vaccinated) as numerator, count(*) as denominator 
FROM {{ ref('int_adult_imms_ts_historical_shingles_dose_2') }}
where cohort = 'TURN_65_AFTER_SEP_2023'
group by all

UNION
--70-79 CATCH UP
select 
vaccine, cohort, analysis_month, fiscal_year_label, practice_code, age_band_5y, ethnicity_category,  imd_quintile, 
sum(vaccinated) as numerator, count(*) as denominator 
FROM {{ ref('int_adult_imms_ts_historical_shingles_dose_2') }}
where cohort = '70-79 CATCH UP'
group by all
) p
