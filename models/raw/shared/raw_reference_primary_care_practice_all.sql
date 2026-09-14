{{
    config(
        description="Raw layer (Curated primary care organisation reference - practices, PCNs, neighbourhoods and memberships). 1:1 passthrough with cleaned column names. \nSource: REFERENCE.PRIMARY_CARE.PRACTICE_ALL \ndbt: source(''reference_primary_care'', ''PRACTICE_ALL'') \nColumns:\n  PRACTICE_CODE -> practice_code\n  PRACTICE_NAME -> practice_name\n  PRACTICE_STATUS -> practice_status\n  POSTCODE -> postcode\n  OPEN_DATE -> open_date\n  CLOSE_DATE -> close_date\n  SUB_ICB_CODE -> sub_icb_code\n  SUB_ICB_NAME -> sub_icb_name\n  HEALTH_BOROUGH_NAME -> health_borough_name\n  REGISTERED_BOROUGH_NAME -> registered_borough_name\n  GEOGRAPHIC_BOROUGH_NAME -> geographic_borough_name\n  ISA_ACCEPTED -> isa_accepted\n  PCN_CODE -> pcn_code\n  PCN_NAME -> pcn_name\n  PCN_NAME_WITH_BOROUGH -> pcn_name_with_borough\n  NEIGHBOURHOOD_CODE -> neighbourhood_code\n  NEIGHBOURHOOD_NAME -> neighbourhood_name\n  NEIGHBOURHOOD_NAME_WITH_BOROUGH -> neighbourhood_name_with_borough\n  UPRN -> uprn\n  LATITUDE -> latitude\n  LONGITUDE -> longitude\n  LSOA -> lsoa\n  MSOA -> msoa\n  CONTACT_PHONE -> contact_phone\n  ADDRESS_LINE_1 -> address_line_1\n  ADDRESS_LINE_2 -> address_line_2\n  ADDRESS_LINE_3 -> address_line_3\n  ADDRESS_LINE_4 -> address_line_4\n  ADDRESS_LINE_5 -> address_line_5\n  ODS_FIRST_CREATED -> ods_first_created\n  ODS_LAST_UPDATED -> ods_last_updated\n  DETAILS_SINCE -> details_since"
    )
}}
select
    "PRACTICE_CODE" as practice_code,
    "PRACTICE_NAME" as practice_name,
    "PRACTICE_STATUS" as practice_status,
    "POSTCODE" as postcode,
    "OPEN_DATE" as open_date,
    "CLOSE_DATE" as close_date,
    "SUB_ICB_CODE" as sub_icb_code,
    "SUB_ICB_NAME" as sub_icb_name,
    "HEALTH_BOROUGH_NAME" as health_borough_name,
    "REGISTERED_BOROUGH_NAME" as registered_borough_name,
    "GEOGRAPHIC_BOROUGH_NAME" as geographic_borough_name,
    "ISA_ACCEPTED" as isa_accepted,
    "PCN_CODE" as pcn_code,
    "PCN_NAME" as pcn_name,
    "PCN_NAME_WITH_BOROUGH" as pcn_name_with_borough,
    "NEIGHBOURHOOD_CODE" as neighbourhood_code,
    "NEIGHBOURHOOD_NAME" as neighbourhood_name,
    "NEIGHBOURHOOD_NAME_WITH_BOROUGH" as neighbourhood_name_with_borough,
    "UPRN" as uprn,
    "LATITUDE" as latitude,
    "LONGITUDE" as longitude,
    "LSOA" as lsoa,
    "MSOA" as msoa,
    "CONTACT_PHONE" as contact_phone,
    "ADDRESS_LINE_1" as address_line_1,
    "ADDRESS_LINE_2" as address_line_2,
    "ADDRESS_LINE_3" as address_line_3,
    "ADDRESS_LINE_4" as address_line_4,
    "ADDRESS_LINE_5" as address_line_5,
    "ODS_FIRST_CREATED" as ods_first_created,
    "ODS_LAST_UPDATED" as ods_last_updated,
    "DETAILS_SINCE" as details_since
from {{ source('reference_primary_care', 'PRACTICE_ALL') }}
