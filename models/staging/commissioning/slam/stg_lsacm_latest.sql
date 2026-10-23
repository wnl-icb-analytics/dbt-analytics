{{
    config(
        materialized = 'view',
        tags = ['sdl', 'slam', 'lsacm']
    )
}}

-- SLAM aggregate contract monitoring (ACM): current provider statement only. One row per source row in the
-- latest upload of each (site, recipient) stream for each provider, FY and
-- month -- sum-safe; superseded restatements excluded. Full history (all submissions) is in
-- stg_lsacm; selection in stg_slam_latest_submission. Where a slice has
-- several selected files, each content group keeps the latest file's rows.

{{ slam_latest_view('stg_lsacm', 'LSACM') }}