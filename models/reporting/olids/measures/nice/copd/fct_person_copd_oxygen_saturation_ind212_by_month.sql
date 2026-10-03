{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind212('by_month') }}
