{{
    config(
        materialized='semantic_view',
        schema='SEMANTIC'
    )
}}

{#
    Cost Index semantic view

    Patient-month cost and activity by service, source and cost basis.
    SLAM and EPD supply actual costs; MHSDS and CSDS supply currency-priced
    proxy costs. Each source retains its available history. Check
    int_cost_index_source_coverage before comparing periods across sources.

    Do not sum across sources or mix actual and proxy costs without checking
    overlap, including SLAM block payments for care priced in MHSDS or CSDS.
    This patient-level view applies no small-number suppression. Aggregate
    outputs and apply small-number suppression before sharing.
#}

TABLES(
    cost AS {{ ref('fct_person_cost_index_monthly') }}
        PRIMARY KEY (sk_patient_id, activity_month, service_grouping, service, is_patient_attributable, cost_basis, cost_source, activity_unit)
        COMMENT = 'WNL (NCL and NWL) monthly patient-level cost and activity. Grain: patient x month x service grouping x service x attributable flag x cost basis x source x activity unit. SLAM and EPD supply actual costs; MHSDS and CSDS supply currency-priced proxy costs. Includes unidentified SLAM activity and non-patient-attributable costs. Source histories differ; check int_cost_index_source_coverage. Do not sum across sources or mix actual and proxy costs without checking overlap. No small-number suppression: aggregate and suppress outputs before sharing.'
)

DIMENSIONS(
    cost.sk_patient_id AS sk_patient_id COMMENT = 'Pseudonymised patient linkage key; NULL for unidentified SLAM activity. Link to sem_olids_population.sk_patient_id after reducing each input to one row per patient or aligned patient-month. Never return this key in final results. Aggregate and apply small-number suppression before sharing.',
    cost.activity_month AS activity_month COMMENT = 'First day of the month: SLAM financial period, EPD prescribing processing period, or MHSDS/CSDS activity month. SLAM starts April 2021 and excludes its latest incomplete submitted month. Coverage differs by source; check int_cost_index_source_coverage.',
    cost.service_grouping AS service_grouping COMMENT = 'Service grouping: Planned, Crisis, Community, Mental Health, Excluded or Unmapped. MHSDS is Mental Health; EPD and CSDS are Community. Unmapped means no SLAM service mapping was found.',
    cost.service AS service COMMENT = 'Mapped SLAM point-of-delivery group, GP Prescriptions (EPD), MH Inpatient, MH Crisis Contact or MH Community Contact (MHSDS), or Community Health Contact (CSDS).',
    cost.is_patient_attributable AS is_patient_attributable COMMENT = 'TRUE for patient-attributable costs; all EPD, MHSDS and CSDS rows are TRUE. FALSE excludes SLAM groups such as CQUIN, transport and block/adjustment costs. Filter TRUE and exclude null patient keys for identified-patient analysis. This flag does not establish that sources are mutually exclusive.',
    cost.cost_basis AS cost_basis COMMENT = 'actual for SLAM agreed provider costs and EPD actual reimbursed GP prescribing costs; proxy for MHSDS and CSDS activity priced using NHS England 2026/27 currencies, adjusted by GDP deflator and provider market forces factor. A currency is an activity category used for pricing. Do not mix actual and proxy costs without checking overlap.',
    cost.cost_source AS cost_source COMMENT = 'Contributing dataset: SLAM (agreed actual provider costs), EPD (actual reimbursed GP prescribing costs), MHSDS (mental health inpatient and contact proxy costs), or CSDS (community contact proxy costs). Do not sum across sources without checking overlap, including SLAM block payments for care also priced in MHSDS or CSDS.',
    cost.activity_unit AS activity_unit COMMENT = 'Unit for total_activity: slam_activity (SLAM), prescription_item (EPD), bed_day (MHSDS inpatient), or contact (MHSDS/CSDS). Keep unlike units separate; contacts include non-costed contacts.'
)

METRICS(
    cost.total_cost AS SUM(cost.total_cost) COMMENT = 'Sum of costs in GBP for the selection: SLAM and EPD actual costs, or MHSDS and CSDS currency-priced proxy estimates. Select or group by cost_source and cost_basis. Do not sum across sources or mix actual and proxy costs without checking overlap, particularly SLAM block payments and MHSDS/CSDS proxies.',
    cost.total_activity AS SUM(cost.total_activity) COMMENT = 'Sum of source activity for the selection, in activity_unit. Keep SLAM activity, prescription items, bed days and contacts separate. MHSDS and CSDS count all contact rows, including non-costed contacts; sources may overlap and do not necessarily count distinct events.',
    cost.patient_count AS COUNT(DISTINCT cost.sk_patient_id) COMMENT = 'Distinct non-null patient linkage keys in the selection. Patients may appear in several services, sources and months; do not sum group counts or use row counts for people. No small-number suppression is applied; suppress small counts before sharing.'
)

COMMENT = 'Monthly patient-level cost and activity for WNL (NCL and NWL). Grain: patient x month x service grouping x service x attributable flag x cost basis x cost source x activity unit. SLAM supplies agreed actual provider costs and EPD actual reimbursed GP prescribing costs. MHSDS mental health inpatient and contacts and CSDS community contacts supply proxy costs based on NHS England 2026/27 currency prices, adjusted by GDP deflator and provider market forces factor. Do not sum across sources or mix actual and proxy costs without checking overlap, including SLAM block payments for care priced in MHSDS or CSDS. A patient appears in several rows; use patient_count, not COUNT(*), for people. activity_month is month-start; align with month-end sem_olids_trends.analysis_month using DATE_TRUNC(month, ...). SLAM starts April 2021 and excludes its latest incomplete submitted month. Each source retains its available history; check int_cost_index_source_coverage for month ranges. No small-number suppression is applied: aggregate outputs and apply small-number suppression before sharing.'
AI_SQL_GENERATION 'This view contains monthly patient-level costs and activity: SLAM and EPD actual costs, MHSDS and CSDS currency-priced proxy estimates. Select or group by cost_source and cost_basis. Do not sum across sources or mix actual and proxy costs without checking overlap; SLAM mental health and community block payments may cover the same care as MHSDS and CSDS proxies. Filter is_patient_attributable = TRUE and exclude null sk_patient_id for identified-patient analysis; the flag does not establish that sources are mutually exclusive. Keep total_activity separate by activity_unit and source; contacts include non-costed contacts. For cross-dataset linkage, query each semantic view in its own CTE, reduce each CTE to one row per sk_patient_id or aligned patient-month before joining, then aggregate. Use patient_count or COUNT(DISTINCT sk_patient_id) for people; group counts are not additive. Keep sk_patient_id out of final output. Align month-start activity_month with month-end sem_olids_trends.analysis_month using DATE_TRUNC(month, ...). Check int_cost_index_source_coverage for shared periods before comparing sources; this view retains separate source histories. SLAM starts April 2021 and excludes its latest incomplete submitted month. No small-number suppression is applied: aggregate outputs and apply small-number suppression before sharing.'
AI_QUESTION_CATEGORIZATION 'Use this view for monthly costs and activity by source, cost basis and service: SLAM agreed actual provider costs, EPD actual reimbursed GP prescribing costs, MHSDS mental health inpatient and contact currency-priced proxy costs, CSDS community contact currency-priced proxy costs, and costs of OLIDS-defined cohorts via sk_patient_id linkage. Check overlap before combining sources or actual and proxy costs. Patient-level data has no small-number suppression; aggregate and suppress outputs before sharing.'
