{% test pcd_refset_volume_guardrail(model, source_value='PCD', max_change_fraction=0.10) %}
    with combined_current as (
        select
            count(*) as row_count,
            count(distinct cluster_id) as cluster_count
        from {{ model }}
        where source = '{{ source_value }}'
    ),

    latest_release as (
        select
            max(snapshot_date) as snapshot_date,
            count(*) as row_count,
            count(distinct cluster_id) as cluster_count,
            count(distinct snapshot_date) as snapshot_date_count,
            count(distinct release_version) as release_version_count,
            count(distinct source_file) as source_file_count
        from {{ ref('stg_reference_pcd_refset_latest') }}
    ),

    prior_release as (
        select
            snapshots.snapshot_date,
            count(*) as row_count,
            count(distinct snapshots.cluster_id) as cluster_count
        from {{ ref('stg_reference_pcd_refset_snapshots') }} as snapshots
        cross join latest_release
        where snapshots.snapshot_date < latest_release.snapshot_date
        group by snapshots.snapshot_date
        qualify row_number() over (order by snapshots.snapshot_date desc) = 1
    ),

    checks as (
        select
            'latest_has_invalid_snapshot_date_count' as issue,
            latest_release.snapshot_date_count as observed_value,
            1 as expected_value
        from latest_release
        where latest_release.snapshot_date_count != 1

        union all

        select
            'latest_has_invalid_release_version_count' as issue,
            latest_release.release_version_count as observed_value,
            1 as expected_value
        from latest_release
        where latest_release.release_version_count != 1

        union all

        select
            'latest_has_invalid_source_file_count' as issue,
            latest_release.source_file_count as observed_value,
            1 as expected_value
        from latest_release
        where latest_release.source_file_count != 1

        union all

        select
            'combined_row_count_differs_from_latest' as issue,
            combined_current.row_count as observed_value,
            latest_release.row_count as expected_value
        from combined_current
        cross join latest_release
        where combined_current.row_count != latest_release.row_count

        union all

        select
            'combined_cluster_count_differs_from_latest' as issue,
            combined_current.cluster_count as observed_value,
            latest_release.cluster_count as expected_value
        from combined_current
        cross join latest_release
        where combined_current.cluster_count != latest_release.cluster_count

        union all

        select
            'latest_row_count_change_exceeds_tolerance' as issue,
            latest_release.row_count as observed_value,
            prior_release.row_count as expected_value
        from latest_release
        cross join prior_release
        where abs(latest_release.row_count - prior_release.row_count)
            / nullif(prior_release.row_count, 0) > {{ max_change_fraction }}

        union all

        select
            'latest_cluster_count_change_exceeds_tolerance' as issue,
            latest_release.cluster_count as observed_value,
            prior_release.cluster_count as expected_value
        from latest_release
        cross join prior_release
        where abs(latest_release.cluster_count - prior_release.cluster_count)
            / nullif(prior_release.cluster_count, 0) > {{ max_change_fraction }}
    )

    select * from checks
{% endtest %}
