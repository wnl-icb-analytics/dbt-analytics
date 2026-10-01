select
    sk_commissioner_id,
    commissioner_name,
    commissioner_type,
    commissioner_code,
    start_date,
    end_date,
    date_created,
    date_updated,
    sk_service_provider_group_id,
    is_customer,
    is_test_organisation
from {{ ref('raw_dictionary_dbo_commissioner') }}
