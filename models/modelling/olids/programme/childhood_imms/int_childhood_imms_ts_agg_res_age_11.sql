{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

select 
p.analysis_month
,p.fiscal_year_label
,p.vaccination_metric
,'11 YEARS' as reporting_age
,CASE
WHEN p.Vaccination_metric ='All vaccinations 11 Years' THEN 1
WHEN p.Vaccination_metric ='6-in-1 (3 doses) 11 Years' THEN 2
WHEN p.Vaccination_metric ='4-in-1 (dose 1) 11 Years' THEN 3
WHEN p.Vaccination_metric ='Hib/MenC 11 Years' THEN 4
WHEN p.Vaccination_metric ='MMR (2 doses) 11 Years' THEN 5
END As VACC_ORDER 
,p.residential_borough
,p.residential_neighbourhood
,p.ward_name
,p.ethnicity_category
,p.ethcat_order
,p.imd_quintile
,p.imdquintile_order
,p.numerator 
,p.denominator 
FROM (

------- 11 YEAR METRICS FROM HISTORICAL 
--sixin1_11y 
select 
 '6-in-1 (3 doses) 11 Years' as vaccination_metric, analysis_month, fiscal_year_label,  residential_borough, residential_neighbourhood, ward_name, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(sixin1_comp_by_11) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_11') }}
group by all

UNION
--hibmenc_11y 
select 
 'Hib/MenC 11 Years' as vaccination_metric, analysis_month, fiscal_year_label,  residential_borough, residential_neighbourhood, ward_name, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(hibmc_comp_by_11) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_11') }}
group by all

UNION
--fourin1_11y 
select 
 '4-in-1 (dose 1) 11 Years' as vaccination_metric, analysis_month, fiscal_year_label,  residential_borough, residential_neighbourhood, ward_name, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(fourin1_comp_by_11) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_11') }}
group by all

UNION
--mmr1_11y 
select 
 'MMR (2 doses) 11 Years' as vaccination_metric, analysis_month, fiscal_year_label,  residential_borough, residential_neighbourhood, ward_name, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(mmr_comp_by_11) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_11') }}
group by all

UNION
--all_vacc11y 
select 
 'All vaccinations 11 Years' as vaccination_metric, analysis_month, fiscal_year_label,  residential_borough, residential_neighbourhood, ward_name, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(all_comp_by_11) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_11') }}
group by all
) p