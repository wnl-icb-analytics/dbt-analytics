{{
    config(
        description="Raw layer (Analyst-managed reference datasets and business rules. TURNAROUND_TIMES_RAW is not in this source; use int_tat_turnaround_times.). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.IMMS_SCHEDULE_ADULT_LATEST \ndbt: source(''reference_analyst_managed'', ''IMMS_SCHEDULE_ADULT_LATEST'') \nColumns:\n  VACCINE_ORDER -> vaccine_order\n  VACCINE_ID -> vaccine_id\n  VACCINE_NAME -> vaccine_name\n  DOSE_NUMBER -> dose_number\n  DISEASES_PROTECTED_AGAINST -> diseases_protected_against\n  TRADE_NAME -> trade_name\n  ADMINISTRATION_ROUTE -> administration_route\n  SCHEDULE_AGE -> schedule_age\n  NEXT_DOSE_VACCINE_ID -> next_dose_vaccine_id\n  ELIGIBLE_AGE_FROM_DAYS -> eligible_age_from_days\n  ELIGIBLE_AGE_TO_DAYS -> eligible_age_to_days\n  MAXIMUM_AGE_DAYS -> maximum_age_days\n  ADMINISTERED_CLUSTER_ID -> administered_cluster_id\n  DRUG_CLUSTER_ID -> drug_cluster_id\n  DECLINED_CLUSTER_ID -> declined_cluster_id\n  CONTRAINDICATED_CLUSTER_ID -> contraindicated_cluster_id"
    )
}}
select
    "VACCINE_ORDER" as vaccine_order,
    "VACCINE_ID" as vaccine_id,
    "VACCINE_NAME" as vaccine_name,
    "DOSE_NUMBER" as dose_number,
    "DISEASES_PROTECTED_AGAINST" as diseases_protected_against,
    "TRADE_NAME" as trade_name,
    "ADMINISTRATION_ROUTE" as administration_route,
    "SCHEDULE_AGE" as schedule_age,
    "NEXT_DOSE_VACCINE_ID" as next_dose_vaccine_id,
    "ELIGIBLE_AGE_FROM_DAYS" as eligible_age_from_days,
    "ELIGIBLE_AGE_TO_DAYS" as eligible_age_to_days,
    "MAXIMUM_AGE_DAYS" as maximum_age_days,
    "ADMINISTERED_CLUSTER_ID" as administered_cluster_id,
    "DRUG_CLUSTER_ID" as drug_cluster_id,
    "DECLINED_CLUSTER_ID" as declined_cluster_id,
    "CONTRAINDICATED_CLUSTER_ID" as contraindicated_cluster_id
from {{ source('reference_analyst_managed', 'IMMS_SCHEDULE_ADULT_LATEST') }}
