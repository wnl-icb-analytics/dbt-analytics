{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind112('by_month') }}
