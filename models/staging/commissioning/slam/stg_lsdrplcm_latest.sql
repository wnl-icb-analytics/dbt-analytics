{{
    config(
        materialized = 'view',
        tags = ['sdl', 'slam', 'lsdrplcm']
    )
}}

-- SLAM high-cost drugs PLD: current provider statement only. One row per source row in the
-- latest file for each (provider, FY, month) slice -- sum-safe; superseded
-- cumulative restatements excluded. Full history (all submissions) is in
-- stg_lsdrplcm; slice resolution in stg_slam_latest_submission.

{{ slam_latest_view('stg_lsdrplcm', 'LSDrPLCM') }}