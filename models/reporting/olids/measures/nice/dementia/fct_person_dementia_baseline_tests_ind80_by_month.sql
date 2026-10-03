{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind80('by_month') }}
