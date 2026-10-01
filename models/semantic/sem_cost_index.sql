{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    Cost Index Semantic View
    ========================

    Patient-month spend by service. SLAM actual provider cost starts in
    2021-04 and excludes the latest incomplete month. EPD prescribing starts
    in 2018-04 but is about 12 months behind SLAM, so recent combined totals
    are asymmetric. About 1.5% of cost is Unmapped service.
#}

TABLES(
    cost AS {{ ref('fct_person_cost_index_monthly') }}
        PRIMARY KEY (sk_patient_id, activity_month, service_grouping, service, is_patient_attributable, cost_basis, cost_source, activity_unit)
        COMMENT = 'Patient-level monthly cost and activity by service'
)

DIMENSIONS(
    cost.sk_patient_id AS sk_patient_id COMMENT = 'Pseudonymised patient key for cross-dataset linkage to sem_olids_population.sk_patient_id. Join CTEs on sk_patient_id, then aggregate. Never return sk_patient_id in final results.',
    cost.activity_month AS activity_month COMMENT = 'First day of the activity month',
    cost.service_grouping AS service_grouping COMMENT = 'Service grouping: Planned, Crisis, Community, Excluded, or Unmapped. Unmapped is ~1.5% of cost.',
    cost.service AS service COMMENT = 'Service',
    cost.is_patient_attributable AS is_patient_attributable COMMENT = 'TRUE for patient-attributable cost. Filter TRUE for patient-level analysis.',
    cost.cost_basis AS cost_basis COMMENT = 'Cost basis. Production currently contains actual only.',
    cost.cost_source AS cost_source COMMENT = 'Cost source: SLAM (acute provider cost) or EPD (prescribing).',
    cost.activity_unit AS activity_unit COMMENT = 'Unit for total_activity: slam_activity or prescription_item.'
)

METRICS(
    cost.total_cost AS SUM(cost.total_cost) COMMENT = 'Total cost',
    cost.total_activity AS SUM(cost.total_activity) COMMENT = 'Total activity',
    cost.patient_count AS COUNT(DISTINCT cost.sk_patient_id) COMMENT = 'Distinct people'
)

COMMENT = 'Cost Index Semantic View - monthly patient-level SLAM and EPD spend. Grain: one row per person x month x service line (service_grouping, service, attributable flag, cost basis, cost source, activity unit) - a person appears many times per month, so never COUNT(*) for people. activity_month is a month-START date (sem_olids_trends.analysis_month is month-END; align on DATE_TRUNC(month, ...)). SLAM actual provider cost starts 2021-04 and excludes the latest incomplete month. EPD prescribing starts 2018-04 but is about 12 months behind, so recent combined totals are asymmetric.'
AI_SQL_GENERATION 'This is monthly patient-level spend. For cross-dataset linkage, query each semantic view in its own CTE, reduce each CTE to one row per sk_patient_id or aligned period before joining, then aggregate. Use COUNT(DISTINCT sk_patient_id) for people and the view metric for events; keep sk_patient_id out of final output. Example: SELECT service_grouping, AGG(patient_count), AGG(total_cost) FROM SEM_COST_INDEX WHERE is_patient_attributable = TRUE GROUP BY service_grouping. Example linkage: reduce an active population cohort in sem_olids_population and spend here before joining on sk_patient_id. SLAM actual cost starts 2021-04 and excludes the latest incomplete month. EPD prescribing starts 2018-04 and is about 12 months behind. About 1.5% of cost is Unmapped service.'
AI_QUESTION_CATEGORIZATION 'Use this view for: monthly spend, activity, service mix, SLAM actual provider cost, EPD prescribing cost, and costs of OLIDS-defined cohorts via sk_patient_id linkage.'
