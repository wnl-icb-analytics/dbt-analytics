{% docs bp_control_evidence_rule %}
Control uses independent systolic and diastolic minima within each context on the latest
eligible clinical date. Components never cross dates or contexts. Clinic readings qualify.
A complete home average takes priority over self-reported or unspecified home readings on
the same date, even when that average is invalid. A lone average component does not block
complete single home readings. Night-time averages and minimum ABPM readings are excluded
before pairing; daytime (day-interval) averages are preferred, and 24-hour averages are a
same-day fallback. Unspecified ABPM ranks between daytime and 24-hour averages. ABPM
preference applies after pairing, with valid complete readings first. Invalid-only days
remain and cannot achieve control. These are local selection rules.
{% enddocs %}
