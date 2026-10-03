{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind272('by_month') }}
