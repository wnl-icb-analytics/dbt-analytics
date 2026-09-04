{% test pcd_refset_matches_latest(model, source_value='PCD') %}
    with combined_pcd as (
        select
            upper(cluster_id) as cluster_id,
            code::varchar as snomed_code
        from {{ model }}
        where source = '{{ source_value }}'
    ),

    latest_pcd as (
        select
            upper(cluster_id) as cluster_id,
            snomed_code::varchar as snomed_code
        from {{ ref('stg_reference_pcd_refset_latest') }}
    ),

    missing_from_combined as (
        select * from latest_pcd
        except
        select * from combined_pcd
    ),

    retired_in_combined as (
        select * from combined_pcd
        except
        select * from latest_pcd
    )

    select 'missing_from_combined' as issue, * from missing_from_combined
    union all
    select 'retired_in_combined' as issue, * from retired_in_combined
{% endtest %}
