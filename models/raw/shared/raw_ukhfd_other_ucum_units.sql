{{
    config(
        description="Raw layer: UCUM expressions and their UKHFD definition history.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.Other.dim_Unified_Code_For_Units_Of_Measure_SCD \ndbt: source(''ukhfd_other'', ''ucum_units'') \nColumns:\n  Code -> code\n  Descriptive_Name -> descriptive_name\n  Code_System -> code_system\n  Definition -> definition\n  Date_Created -> date_created\n  Synonym -> synonym\n  Status -> status\n  Kind_Of_Quantity -> kind_of_quantity\n  Date_Revised -> date_revised\n  ConceptID -> concept_id\n  Dimension -> dimension\n  In_Source_Data -> in_source_data\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Is_Latest -> is_latest\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "Code" as code,
    "Descriptive_Name" as descriptive_name,
    "Code_System" as code_system,
    "Definition" as definition,
    "Date_Created" as date_created,
    "Synonym" as synonym,
    "Status" as status,
    "Kind_Of_Quantity" as kind_of_quantity,
    "Date_Revised" as date_revised,
    "ConceptID" as concept_id,
    "Dimension" as dimension,
    "In_Source_Data" as in_source_data,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Is_Latest" as is_latest,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_other', 'ucum_units') }}
