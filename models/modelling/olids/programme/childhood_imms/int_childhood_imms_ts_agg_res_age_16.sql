{{
    config(
        materialized='table',
        tags=['childhood_imms'])
}}

select 
p.analysis_month
,p.fiscal_year_label
,p.vaccination_metric
,'16 YEARS' as reporting_age
,CASE
WHEN p.Vaccination_metric ='All vaccinations 16 Years' THEN 6
WHEN p.Vaccination_metric ='6-in-1 (3 doses) 16 Years' THEN 7
WHEN p.Vaccination_metric ='4-in-1 (dose 1) 16 Years' THEN 8
WHEN p.Vaccination_metric ='MMR (2 doses) 16 Years' THEN 9
WHEN p.Vaccination_metric ='HPV (dose 1) 16 Years' THEN 10
WHEN p.Vaccination_metric ='3-in-1 (dose 1) 16 Years' THEN 11
WHEN p.Vaccination_metric ='MenACWY (dose 1) 16 Years' THEN 12
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

------- 16 YEAR METRICS FROM HISTORICAL 
--sixin1_16y 
select 
 '6-in-1 (3 doses) 16 Years' as vaccination_metric, analysis_month, fiscal_year_label,  residential_borough, residential_neighbourhood, ward_name, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(sixin1_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_16') }}
group by all

UNION
--fourin1_16y 
select 
 '4-in-1 (dose 1) 16 Years' as vaccination_metric, analysis_month, fiscal_year_label,  residential_borough, residential_neighbourhood, ward_name, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(fourin1_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_16') }}
group by all

UNION
--mmr1_16y 
select 
 'MMR (2 doses) 16 Years' as vaccination_metric, analysis_month, fiscal_year_label,  residential_borough, residential_neighbourhood, ward_name, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(mmr_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_16') }}
group by all

UNION
--HPV_16y 
select 
 'HPV (dose 1) 16 Years' as vaccination_metric, analysis_month, fiscal_year_label,  residential_borough, residential_neighbourhood, ward_name, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(hpv_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_16') }}
group by all

UNION
--threein1_16y 
select 
 '3-in-1 (dose 1) 16 Years' as vaccination_metric, analysis_month, fiscal_year_label,  residential_borough, residential_neighbourhood, ward_name, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(threein1_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_16') }}
group by all

UNION
--MenACWY_16y 
select 
 'MenACWY (dose 1) 16 Years' as vaccination_metric, analysis_month, fiscal_year_label,  residential_borough, residential_neighbourhood, ward_name, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(menacwy_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_16') }}
group by all

UNION
--all_vacc16y 
select 
 'All vaccinations 16 Years' as vaccination_metric, analysis_month, fiscal_year_label,  residential_borough, residential_neighbourhood, ward_name, ethcat_order, ethnicity_category, imd_quintile, imdquintile_order, 
sum(all_comp_by_16) as numerator, count(*) as denominator 
FROM {{ ref('int_childhood_imms_vaccs_historical_age_16') }}
group by all

) p
