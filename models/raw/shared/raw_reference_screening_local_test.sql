{{
    config(
        description="Raw layer (Cancer screening data). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.CANCER__SCREENING.SCREENING_LOCAL_TEST \ndbt: source(''reference_cancer_screening'', ''SCREENING_LOCAL_TEST'') \nColumns:\n  METRIC_ID -> metric_id\n  METRIC_NAME -> metric_name\n  GEOGRAPHY_TYPE -> geography_type\n  GP_CODE -> gp_code\n  GP_NAME -> gp_name\n  ICB_CODE -> icb_code\n  ICB_NAME -> icb_name\n  REPORTING_END_DATE -> reporting_end_date\n  NUMERATOR -> numerator\n  DENOMINATOR -> denominator\n  VALUE -> value\n  LOWER_LIMIT_95 -> lower_limit_95\n  UPPER_LIMIT_95 -> upper_limit_95\n  LOWER_LIMIT_99_7 -> lower_limit_99_7\n  UPPER_LIMIT_99_7 -> upper_limit_99_7\n  GP_IMD -> gp_imd\n  _TIMESTAMP -> timestamp"
    )
}}
select
    "METRIC_ID" as metric_id,
    "METRIC_NAME" as metric_name,
    "GEOGRAPHY_TYPE" as geography_type,
    "GP_CODE" as gp_code,
    "GP_NAME" as gp_name,
    "ICB_CODE" as icb_code,
    "ICB_NAME" as icb_name,
    "REPORTING_END_DATE" as reporting_end_date,
    "NUMERATOR" as numerator,
    "DENOMINATOR" as denominator,
    "VALUE" as value,
    "LOWER_LIMIT_95" as lower_limit_95,
    "UPPER_LIMIT_95" as upper_limit_95,
    "LOWER_LIMIT_99_7" as lower_limit_99_7,
    "UPPER_LIMIT_99_7" as upper_limit_99_7,
    "GP_IMD" as gp_imd,
    "_TIMESTAMP" as timestamp
from {{ source('reference_cancer_screening', 'SCREENING_LOCAL_TEST') }}
