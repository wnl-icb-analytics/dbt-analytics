{{
    config(
        description="Raw layer: Combined Contract IAP 2026-27 - Reference lookup table for analyst-managed. 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE__NCL.ANALYST_MANAGED.COMBINED_CONTRACT_IAP_202627 \ndbt: source(''reference_analyst_managed'', ''COMBINED_CONTRACT_IAP_202627'') \nColumns:\n  FINANCIAL_MONTH -> financial_month\n  FINANCIAL_YEAR -> financial_year\n  DATA_SET_CREATED -> data_set_created\n  PROVIDER_CODE -> provider_code\n  COMMISSIONER_CODE -> commissioner_code\n  COMMISSIONED_SERVICE_CATEGORY_CODE -> commissioned_service_category_code\n  SERVICE_CODE -> service_code\n  POINT_OF_DELIVERY_CODE -> point_of_delivery_code\n  TARIFF_CODE -> tariff_code\n  CONTRACT_MONITORING_PLANNED_ACTIVITY -> contract_monitoring_planned_activity\n  CONTRACT_MONITORING_PLANNED_PRICE -> contract_monitoring_planned_price\n  SOURCE_PROVIDER -> source_provider\n  SOURCE_FILE -> source_file\n  TYPE -> type\n  CONTRACT_TYPE -> contract_type"
    )
}}
select
    "FINANCIAL_MONTH" as financial_month,
    "FINANCIAL_YEAR" as financial_year,
    "DATA_SET_CREATED" as data_set_created,
    "PROVIDER_CODE" as provider_code,
    "COMMISSIONER_CODE" as commissioner_code,
    "COMMISSIONED_SERVICE_CATEGORY_CODE" as commissioned_service_category_code,
    "SERVICE_CODE" as service_code,
    "POINT_OF_DELIVERY_CODE" as point_of_delivery_code,
    "TARIFF_CODE" as tariff_code,
    "CONTRACT_MONITORING_PLANNED_ACTIVITY" as contract_monitoring_planned_activity,
    "CONTRACT_MONITORING_PLANNED_PRICE" as contract_monitoring_planned_price,
    "SOURCE_PROVIDER" as source_provider,
    "SOURCE_FILE" as source_file,
    "TYPE" as type,
    "CONTRACT_TYPE" as contract_type
from {{ source('reference_analyst_managed', 'COMBINED_CONTRACT_IAP_202627') }}
