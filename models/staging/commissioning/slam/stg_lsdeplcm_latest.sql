{{
    config(
        materialized = 'view',
        tags = ['sdl', 'slam', 'lsdeplcm']
    )
}}

-- SLAM high-cost devices PLD: current provider statement only. One row per source row in the
-- latest file for each (provider, FY, month) slice -- sum-safe; superseded
-- cumulative restatements excluded. Full history (all submissions) is in
-- stg_lsdeplcm; slice resolution in stg_slam_latest_submission.

{{ slam_latest_view('stg_lsdeplcm', 'LSDePLCM') }}