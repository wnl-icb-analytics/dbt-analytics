{{
    config(
        description="Raw layer: HRG4 chapter labels by financial year, from HRG4+ grouper code-to-group files.. 1:1 passthrough with cleaned column names. \nSource: UKHFD.PbR.dim_HRG4_Chapter \ndbt: source(''ukhfd_pbr_hrg'', ''hrg4_chapter'') \nColumns:\n  HRG4Chapter -> hrg4_chapter\n  HRG4ChapterDescription -> hrg4_chapter_description\n  File_Name -> file_name\n  Import_Date -> import_date\n  Created_Date -> created_date\n  Effective_From -> effective_from\n  Effective_To -> effective_to"
    )
}}
select
    "HRG4Chapter" as hrg4_chapter,
    "HRG4ChapterDescription" as hrg4_chapter_description,
    "File_Name" as file_name,
    "Import_Date" as import_date,
    "Created_Date" as created_date,
    "Effective_From" as effective_from,
    "Effective_To" as effective_to
from {{ source('ukhfd_pbr_hrg', 'hrg4_chapter') }}
