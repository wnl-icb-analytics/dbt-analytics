-- The immunosuppression admin lookback must match the rule the campaign was reported under.
--
-- Flu spec v16.0 widened IMMADM_DAT from a fixed six-month floor to any code in the three
-- years before AUDITEND_DAT. immuno_admin_lookback_date carries that per campaign, so a
-- closed season keeps the six-month rule and 2026-27 onward gets three years. This asserts
-- the config still says so: a silent revert to the medication lookback would leave the
-- widened pathway looking healthy while quietly reading a six-month window.

WITH campaigns AS (
    {{ flu_reported_campaigns() }}
)

SELECT
    campaign_id,
    immuno_admin_lookback_date,
    immuno_medication_lookback_date,
    audit_end_date,
    'Immunosuppression admin lookback does not match the rule for this campaign' AS failure_reason
FROM campaigns
WHERE
    -- 2026-27 onward: three years before the audit end
    (campaign_id >= 'Flu 2026-27'
        AND immuno_admin_lookback_date <> DATEADD('year', -3, audit_end_date))
    -- earlier seasons: the same fixed six-month floor as the medication lookback
    OR (campaign_id < 'Flu 2026-27'
        AND immuno_admin_lookback_date <> immuno_medication_lookback_date)
