{{
    config(
        description="Raw layer (SUS emergency care attendances and activity). 1:1 passthrough with cleaned column names. \nSource: DATA_LAKE.SUS_UNIFIED_ECDS.commissioning.service_agreements \ndbt: source(''sus_ecds'', ''commissioning.service_agreements'') \nColumns:\n  ROWNUMBER_ID -> rownumber_id\n  PRIMARYKEY_ID -> primarykey_id\n  SERVICE_AGREEMENTS_ID -> service_agreements_id\n  commissioner -> commissioner\n  is_commissioner_recognised -> is_commissioner_recognised\n  sha_commissioner -> sha_commissioner\n  commissioner_assignment_period_start_date -> commissioner_assignment_period_start_date\n  commissioner_assignment_period_end_date -> commissioner_assignment_period_end_date\n  commissioning_serial_number -> commissioning_serial_number\n  line_number -> line_number\n  commissioner_reference_number -> commissioner_reference_number\n  provider_reference_number -> provider_reference_number\n  service_code -> service_code\n  dmicImportLogId -> dmic_import_log_id"
    )
}}
select
    "ROWNUMBER_ID" as rownumber_id,
    "PRIMARYKEY_ID" as primarykey_id,
    "SERVICE_AGREEMENTS_ID" as service_agreements_id,
    "commissioner" as commissioner,
    "is_commissioner_recognised" as is_commissioner_recognised,
    "sha_commissioner" as sha_commissioner,
    "commissioner_assignment_period_start_date" as commissioner_assignment_period_start_date,
    "commissioner_assignment_period_end_date" as commissioner_assignment_period_end_date,
    "commissioning_serial_number" as commissioning_serial_number,
    "line_number" as line_number,
    "commissioner_reference_number" as commissioner_reference_number,
    "provider_reference_number" as provider_reference_number,
    "service_code" as service_code,
    "dmicImportLogId" as dmic_import_log_id
from {{ source('sus_ecds', 'commissioning.service_agreements') }}
