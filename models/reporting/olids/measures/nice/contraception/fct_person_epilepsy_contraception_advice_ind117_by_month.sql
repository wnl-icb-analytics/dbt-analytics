{{ config(materialized='view', tags=['monthly-full', 'nice-history']) }}

{{ nice_ind117('by_month') }}
