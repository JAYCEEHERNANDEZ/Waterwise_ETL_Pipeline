-- Multi-device support: replace single device_fingerprint_hash column
-- with a junction table that allows multiple concurrent sessions per user.

CREATE TABLE IF NOT EXISTS user_device_sessions (
  id BIGSERIAL PRIMARY KEY,
  user_id INTEGER NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('consumer', 'admin', 'super-admin', 'meter-reader')),
  fingerprint_hash VARCHAR(64) NOT NULL,
  device_info JSONB DEFAULT '{}',
  created_at TIMESTAMPTZ DEFAULT now(),
  last_used_at TIMESTAMPTZ DEFAULT now(),
  UNIQUE(user_id, role, fingerprint_hash)
);

CREATE INDEX IF NOT EXISTS idx_user_device_sessions_lookup
ON user_device_sessions (user_id, role);

-- Migrate existing single-column data into the new table
INSERT INTO user_device_sessions (user_id, role, fingerprint_hash)
SELECT id, 'admin', device_fingerprint_hash FROM admins
WHERE device_fingerprint_hash IS NOT NULL
ON CONFLICT (user_id, role, fingerprint_hash) DO NOTHING;

INSERT INTO user_device_sessions (user_id, role, fingerprint_hash)
SELECT id, 'consumer', device_fingerprint_hash FROM consumers
WHERE device_fingerprint_hash IS NOT NULL
ON CONFLICT (user_id, role, fingerprint_hash) DO NOTHING;

INSERT INTO user_device_sessions (user_id, role, fingerprint_hash)
SELECT id, 'meter-reader', device_fingerprint_hash FROM meter_readers
WHERE device_fingerprint_hash IS NOT NULL
ON CONFLICT (user_id, role, fingerprint_hash) DO NOTHING;

-- RLS: block direct browser access, backend uses service-role key
ALTER TABLE user_device_sessions ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE user_device_sessions FROM anon, authenticated;
