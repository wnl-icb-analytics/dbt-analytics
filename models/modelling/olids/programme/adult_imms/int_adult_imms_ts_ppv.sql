{{
    config(
        materialized='table',
        tags=['adult_imms'])
}}

--aggregated output one dose 2 cohorts. Aggregate 9.6 million rows to 1.1 million rows
select 
p.vaccine
,p.cohort
,p.analysis_month
,p.fiscal_year_label
,CASE
WHEN p.cohort ='AGE 65+' THEN 1
WHEN p.cohort ='CLINICAL RISK 18-64' THEN 2
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
        WHEN p.imd_quintile_25 = 'Most Deprived' THEN 1
        WHEN p.imd_quintile_25 = 'Second Most Deprived' THEN 2
        WHEN p.imd_quintile_25 = 'Third Most Deprived' THEN 3
        WHEN p.imd_quintile_25 = 'Second Least Deprived' THEN 4
        WHEN p.imd_quintile_25 = 'Least Deprived' THEN 5
        ELSE 6
    END AS imdquintile_order
,p.numerator 
,p.denominator 
FROM (
--Age 65+ 
select 
vaccine, cohort, analysis_month, fiscal_year_label, practice_code, age_band_5y, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, 
sum(vaccinated) as numerator, count(*) as denominator 
FROM {{ ref('int_adult_imms_ts_historical_ppv') }}
where cohort = 'AGE 65+'
group by all

UNION
--18-64 clinical risk
select 
vaccine, cohort, analysis_month, fiscal_year_label, practice_code, age_band_5y, ethnicity_category, ethcat_order, imd_quintile, imdquintile_order, 
sum(vaccinated) as numerator, count(*) as denominator 
FROM {{ ref('int_adult_imms_ts_historical_ppv') }}
where cohort = 'CLINICAL RISK 18-64'
group by all
) p