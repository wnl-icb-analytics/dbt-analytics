-- UKHFD supplies one row per LSOA, measure and release. Pivot back to the
-- published File 7 layout. Measure names vary slightly between releases, so
-- columns match on the name prefix.
--
-- Source: UKHFD.Demography.fact_Domains_Of_Deprivation_By_LSOA_V3
--   source('ukhfd_demography', 'domains_of_deprivation_by_lsoa') -> raw_ukhfd_domains_of_deprivation_by_lsoa

{%- set domains = [
    ('imd', 'Index_of_Multiple_Deprivation_(IMD)'),
    ('income', 'Income'),
    ('employment', 'Employment'),
    ('education_skills_and_training', 'Education,_Skills_and_Training'),
    ('health_deprivation_and_disability', 'Health_Deprivation_and_Disability'),
    ('crime', 'Crime'),
    ('barriers_to_housing_and_services', 'Barriers_to_Housing_and_Services'),
    ('living_environment', 'Living_Environment'),
    ('idaci', 'Income_Deprivation_Affecting_Children_Index_(IDACI)'),
    ('idaopi', 'Income_Deprivation_Affecting_Older_People_(IDAOPI)'),
    ('children_and_young_people', 'Children_and_Young_People_Sub-domain'),
    ('adult_skills', 'Adult_Skills_Sub-domain'),
    ('geographical_barriers', 'Geographical_Barriers_Sub-domain'),
    ('wider_barriers', 'Wider_Barriers_Sub-domain'),
    ('indoors', 'Indoors_Sub-domain'),
    ('outdoors', 'Outdoors_Sub-domain'),
] -%}

{%- set populations = [
    ('total_population', 'Total_population'),
    ('children_population', 'Dependent_Children_aged_0-15'),
    ('population_aged_16_to_59', 'Population_aged_16-59'),
    ('older_population', 'Older_population_aged_60_and_over'),
    ('working_age_population', 'Working_age_population'),
] %}

with measures as (
    select
        year(effective_snapshot_date) as release_year,
        lsoa_code_year::integer as lsoa_boundary_year,
        lsoa_code,
        measure_name,
        measure_value
    from {{ ref('raw_ukhfd_domains_of_deprivation_by_lsoa') }}
    qualify data_source_file_for_this_snapshot_version
        = max(data_source_file_for_this_snapshot_version) over (partition by effective_snapshot_date)
)

select
    release_year,
    lsoa_boundary_year,
    lsoa_code,
    {%- for column, label in domains %}
    max(iff(startswith(measure_name, '{{ label }}_Score'), measure_value, null))::number(38, 3) as {{ column }}_score,
    max(iff(startswith(measure_name, '{{ label }}_Rank'), measure_value, null))::integer as {{ column }}_rank,
    max(iff(startswith(measure_name, '{{ label }}_Decile'), measure_value, null))::integer as {{ column }}_decile,
    {%- endfor %}
    {%- for column, label in populations %}
    max(iff(startswith(measure_name, '{{ label }}'), measure_value, null))::integer as {{ column }}{{ "," if not loop.last }}
    {%- endfor %}
from measures
group by release_year, lsoa_boundary_year, lsoa_code
