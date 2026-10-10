{{
    config(
        description="Raw layer (Curated primary care organisation reference - practices, PCNs, neighbourhoods and memberships). 1:1 passthrough with cleaned column names. \nSource: REFERENCE.PRIMARY_CARE.PRACTICE_HISTORY \ndbt: source(''reference_primary_care'', ''PRACTICE_HISTORY'') \nColumns:\n  PRACTICE_CODE -> practice_code\n  PRACTICE_NAME -> practice_name\n  PRACTICE_STATUS -> practice_status\n  PRESCRIBING_SETTING -> prescribing_setting\n  POSTCODE -> postcode\n  OPEN_DATE -> open_date\n  CLOSE_DATE -> close_date\n  SUB_ICB_CODE -> sub_icb_code\n  SUB_ICB_NAME -> sub_icb_name\n  REGISTERED_BOROUGH_NAME -> registered_borough_name\n  GEOGRAPHIC_BOROUGH_NAME -> geographic_borough_name\n  UPRN -> uprn\n  LATITUDE -> latitude\n  LONGITUDE -> longitude\n  LSOA -> lsoa\n  MSOA -> msoa\n  ADDRESS_LINE_1 -> address_line_1\n  ADDRESS_LINE_2 -> address_line_2\n  ADDRESS_LINE_3 -> address_line_3\n  ADDRESS_LINE_4 -> address_line_4\n  ADDRESS_LINE_5 -> address_line_5\n  ODS_FIRST_CREATED -> ods_first_created\n  ODS_LAST_UPDATED -> ods_last_updated\n  VALID_FROM -> valid_from\n  VALID_TO -> valid_to\n  IS_CURRENT -> is_current\n  SOURCE_SYSTEM -> source_system\n  CHANGE_REASON -> change_reason\n  CREATED_BY -> created_by\n  CREATED_AT -> created_at\n  CONTACT_PHONE -> contact_phone\n  HEALTH_BOROUGH_NAME -> health_borough_name\n  ISA_ACCEPTED -> isa_accepted"
    )
}}
select
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_NAME" as practice_name,
    "PRACTICE_STATUS" as practice_status,
    "PRESCRIBING_SETTING" as prescribing_setting,
    "POSTCODE" as postcode,
    "OPEN_DATE" as open_date,
    "CLOSE_DATE" as close_date,
    "SUB_ICB_CODE" as sub_icb_code,
    "SUB_ICB_NAME" as sub_icb_name,
    "REGISTERED_BOROUGH_NAME" as registered_borough_name,
    "GEOGRAPHIC_BOROUGH_NAME" as geographic_borough_name,
    "UPRN" as uprn,
    "LATITUDE" as latitude,
    "LONGITUDE" as longitude,
    "LSOA" as lsoa,
    "MSOA" as msoa,
    "ADDRESS_LINE_1" as address_line_1,
    "ADDRESS_LINE_2" as address_line_2,
    "ADDRESS_LINE_3" as address_line_3,
    "ADDRESS_LINE_4" as address_line_4,
    "ADDRESS_LINE_5" as address_line_5,
    "ODS_FIRST_CREATED" as ods_first_created,
    "ODS_LAST_UPDATED" as ods_last_updated,
    "VALID_FROM" as valid_from,
    "VALID_TO" as valid_to,
    "IS_CURRENT" as is_current,
    "SOURCE_SYSTEM" as source_system,
    "CHANGE_REASON" as change_reason,
    "CREATED_BY" as created_by,
    "CREATED_AT" as created_at,
    "CONTACT_PHONE" as contact_phone,
    "HEALTH_BOROUGH_NAME" as health_borough_name,
    "ISA_ACCEPTED" as isa_accepted
from {{ source('reference_primary_care', 'PRACTICE_HISTORY') }}
