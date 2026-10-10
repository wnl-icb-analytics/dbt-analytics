{{ config(materialized='table') }}

-- Common long-form interface for the NICE severe mental illness indicator views.
-- Detail columns a view does not emit are typed nulls for its branch.
{{ nice_smi_union('current') }}
