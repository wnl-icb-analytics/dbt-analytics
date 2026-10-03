{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind108('by_month') }}
