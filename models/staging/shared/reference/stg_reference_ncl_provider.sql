-- Staging model for reference_analyst_managed.IMD2019
-- Source: "MODELLING"."LOOKUP_NCL"
-- Description: Analyst-managed reference datasets and business rules

select
    "SK_ORGANISATION_ID" as sk_organisation_id,
    "PROVIDER_CODE" as provider_code,
    "PROVIDER_NAME" as provider_name,
    "PROVIDER_SHORTHAND" as provider_shorthand,
    "REPORTING_CODE" as reporting_code,
    "REPORTING_NAME" as reporting_name,
    "PROVIDER_TYPE" as provider_type,
    "ROW_TYPE" as row_type
from {{ source('reference_lookup_ncl', 'NCL_PROVIDER') }}