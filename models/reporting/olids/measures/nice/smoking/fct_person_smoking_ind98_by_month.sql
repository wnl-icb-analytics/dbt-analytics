{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind98('by_month') }}
