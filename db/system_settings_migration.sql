-- SYSTEM SETTINGS TABLE
-- Stores configurable system-wide settings (e.g., water rate per cubic meter).
-- Only accessible by super-admin via API; all changes require OTP verification.

CREATE TABLE IF NOT EXISTS system_settings (
    id SERIAL PRIMARY KEY,
    setting_key VARCHAR(100) NOT NULL UNIQUE,
    setting_value NUMERIC(12, 2) NOT NULL,
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Seed the default water rate
INSERT INTO system_settings (setting_key, setting_value, description)
VALUES ('rate_per_cubic_meter', 15.00, 'Water consumption rate in PHP per cubic meter')
ON CONFLICT (setting_key) DO NOTHING;

-- Index for fast key lookups
CREATE INDEX IF NOT EXISTS idx_system_settings_key ON system_settings (setting_key);

-- RLS
ALTER TABLE public.system_settings ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE public.system_settings FROM anon, authenticated;
