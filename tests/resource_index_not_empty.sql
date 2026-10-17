{{ config(severity = 'error') }}

-- Grain and column tests pass vacuously when there is no exposure population.
select count(*) as row_count
from {{ ref('fct_person_resource_index') }}
having count(*) = 0
