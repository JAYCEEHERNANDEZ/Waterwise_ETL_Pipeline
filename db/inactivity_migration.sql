-- WaterWise 15-minute inactivity session expiry.
-- Run this in the Supabase SQL Editor. It only adds a nullable timestamp
-- that records the last user-initiated activity per account. Existing rows
-- keep NULL (treated as "no idle enforcement yet") until the next login or
-- activity ping overwrites them.

ALTER TABLE admins ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMPTZ;
ALTER TABLE consumers ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMPTZ;
ALTER TABLE meter_readers ADD COLUMN IF NOT EXISTS last_activity_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_admins_last_activity_at ON admins (last_activity_at);
CREATE INDEX IF NOT EXISTS idx_consumers_last_activity_at ON consumers (last_activity_at);
CREATE INDEX IF NOT EXISTS idx_meter_readers_last_activity_at ON meter_readers (last_activity_at);
