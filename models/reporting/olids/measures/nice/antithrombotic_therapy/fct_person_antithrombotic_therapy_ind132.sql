{{ config(materialized='table') }}

-- NICE IND132: https://www.nice.org.uk/indicators/ind132
-- Antiplatelet or oral anticoagulant evidence in 12 months on the CHD register.
-- Excludes people contraindicated to all three of salicylates, clopidogrel and oral anticoagulants (persisting at any time or expiring in 12 months), as NICE lists and QOF CHD005 applies.
{{ nice_ind132('current') }}
