-- Use the shared historical ODS lookup first; e-RS also has application organisation codes.
select organisation_code, organisation_name, name_source
from {{ ref('organisation') }}
union all
select
    e.organisation_id as organisation_code,
    nullif(trim(e.organisation_name), '') as organisation_name,
    'Dictionary.E-Referral' as name_source
from {{ ref('stg_dictionary_ers_organisation') }} as e
where not exists (
    select 1 from {{ ref('organisation') }} as o
    where o.organisation_code = e.organisation_id
)
