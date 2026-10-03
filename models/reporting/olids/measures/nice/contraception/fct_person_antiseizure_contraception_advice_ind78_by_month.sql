{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind78('by_month') }}
