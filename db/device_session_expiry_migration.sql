-- Device session expiry: bound each enrolled device to the role's absolute
-- session lifetime (refresh-token lifetime per role; 7 days for meter
-- readers who have no refresh token). Mirrors the admin_trusted_devices
-- expires_at pattern.
--
-- Run this BEFORE deploying the backend code that writes expires_at.
-- Backfill is a clean slate from now(): every login re-enrolls the device
-- row (upsert on user_id, role, fingerprint_hash), so existing sessions
-- self-heal on next sign-in. Rows with NULL expires_at are treated as
-- expired by the backend filter.
--
-- RLS is already enabled on user_device_sessions with anon/authenticated
-- revoked (see multi_device_migration.sql); the new column inherits it.

ALTER TABLE user_device_sessions
  ADD COLUMN IF NOT EXISTS expires_at TIMESTAMPTZ;

-- Backfill per role, matching backend/.env refresh lifetimes and the
-- confirmed 7-day meter-reader device lifetime.
UPDATE user_device_sessions
SET expires_at = now() + INTERVAL '90 days'
WHERE role = 'consumer' AND expires_at IS NULL;

UPDATE user_device_sessions
SET expires_at = now() + INTERVAL '1 hour'
WHERE role = 'admin' AND expires_at IS NULL;

UPDATE user_device_sessions
SET expires_at = now() + INTERVAL '30 minutes'
WHERE role = 'super-admin' AND expires_at IS NULL;

UPDATE user_device_sessions
SET expires_at = now() + INTERVAL '7 days'
WHERE role = 'meter-reader' AND expires_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_user_device_sessions_expiry
  ON user_device_sessions (expires_at);
