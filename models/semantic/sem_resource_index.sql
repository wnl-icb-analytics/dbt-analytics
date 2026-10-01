{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    Resource Index Semantic View
    ============================

    Person-level resource index over the latest complete rolling 12-month
    SLAM window. Actual cost is SLAM acute only; EPD prescribing is excluded.
    Resource ratio is SUM(actual_cost_12m) / SUM(expected_cost_12m), never an
    average of person-level ratios. Expected cost is Carr-Hill practice
    allocation and lags the cost window by about one year.
#}

TABLES(
    ri AS {{ ref('fct_person_resource_index') }}
        PRIMARY KEY (sk_patient_id)
        COMMENT = 'Person-level resource index over the latest rolling 12-month cost window'
)

DIMENSIONS(
    ri.sk_patient_id AS sk_patient_id COMMENT = 'Pseudonymised patient key for cross-dataset linkage to sem_olids_population.sk_patient_id. Join CTEs on sk_patient_id, then aggregate. Never return sk_patient_id in final results.',
    ri.gender AS gender COMMENT = 'Gender at the resource-index window end',
    ri.age AS age COMMENT = 'Age at window end or death',
    ri.age_band AS age_band COMMENT = 'NHS age band used for the cost curve',
    ri.ethnicity AS ethnicity COMMENT = 'Ethnicity',
    ri.months_registered AS months_registered COMMENT = 'Months registered in the rolling window',
    ri.first_month_registered AS first_month_registered COMMENT = 'First registered month in the window',
    ri.last_month_registered AS last_month_registered COMMENT = 'Last registered month in the window',
    ri.died_in_window AS died_in_window COMMENT = 'Died inside the rolling window',
    ri.practice_code AS practice_code COMMENT = 'Practice at the last registered month',
    ri.registered_borough AS registered_borough COMMENT = 'Borough of the window practice',
    ri.registered_neighbourhood_name AS registered_neighbourhood_name COMMENT = 'Neighbourhood of the window practice',
    ri.registered_sub_icb_code AS registered_sub_icb_code COMMENT = 'Sub-ICB code of the window practice',
    ri.registered_sub_icb_name AS registered_sub_icb_name COMMENT = 'Sub-ICB name of the window practice',
    ri.residence_imd_decile AS residence_imd_decile COMMENT = 'Residence IMD decile',
    ri.residence_imd_quintile AS residence_imd_quintile COMMENT = 'Residence IMD quintile',
    ri.residence_borough AS residence_borough COMMENT = 'Residence borough',
    ri.residence_ward_2025_name AS residence_ward_2025_name COMMENT = 'Residence ward',
    ri.weighted_ratio_imputed AS weighted_ratio_imputed COMMENT = 'TRUE when the practice had no published Carr-Hill ratio and the WNL mean was used instead.'
)

METRICS(
    ri.total_actual_cost_12m AS SUM(ri.actual_cost_12m) COMMENT = 'Total actual SLAM acute cost in the rolling 12-month window',
    ri.total_expected_cost_12m AS SUM(ri.expected_cost_12m) COMMENT = 'Total expected cost in the rolling 12-month window',
    ri.total_weighted_months AS SUM(ri.weighted_months_12m) COMMENT = 'Total Carr-Hill weighted months',
    ri.patient_count AS COUNT(DISTINCT ri.sk_patient_id) COMMENT = 'Distinct people'
)

COMMENT = 'Resource Index Semantic View - person-level rolling 12-month SLAM acute resource index. Grain: one row per person. Calculate resource ratio as AGG(total_actual_cost_12m) / AGG(total_expected_cost_12m), never an average of person-level ratios. Carr-Hill expected cost is a practice-average allocation lagging the cost window by about one year.'
AI_SQL_GENERATION 'Resource ratio is AGG(total_actual_cost_12m) / AGG(total_expected_cost_12m); never average person-level ratios. For cross-dataset linkage, query each semantic view in its own CTE, reduce each CTE to one row per sk_patient_id or aligned period before joining, then aggregate. Use COUNT(DISTINCT sk_patient_id) for people and the view metric for events; keep sk_patient_id out of final output. Example: SELECT residence_borough, AGG(total_actual_cost_12m), AGG(total_expected_cost_12m) FROM SEM_RESOURCE_INDEX GROUP BY residence_borough. weighted_ratio_imputed marks people whose practice had no published ratio and used the WNL fallback. Example linkage: reduce an active population cohort in sem_olids_population and resource use here before joining on sk_patient_id. Actual cost is SLAM acute only. The rolling window ends at the latest complete SLAM month. Carr-Hill expected cost lags the cost window by about one year.'
AI_QUESTION_CATEGORIZATION 'Use this view for: rolling 12-month resource use, actual versus expected cost, resource ratios by demographic or geography, Carr-Hill weighted population, and resource use of OLIDS-defined cohorts via sk_patient_id linkage.'
