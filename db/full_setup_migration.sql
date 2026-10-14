-- ===============================================================================
-- WaterWise FULL SETUP MIGRATION (consolidated - schema only)
-- ===============================================================================
-- Pinagsama-samang schema migrations sa tamang order.
-- Old files HINDI binura - ito ay bagong file lang para sa one-shot setup.
-- HINDI kasama: seeds (*seed*), audit, remove_future cleanup,
-- fingerprint_migration.sql at drop_old_fingerprint_columns.sql (sadyang
-- tinanggal; user_device_sessions junction table ang source of truth).
--
-- GAMIT: i-run ang buong file nang isang beses sa Supabase SQL Editor.
-- Lahat ng statements ay idempotent (IF NOT EXISTS), kaya ligtas i-re-run.
--
-- ORDER:
--   1. migration.sql
--   2. system_settings_migration.sql
--   3. inactivity_migration.sql
--   4. multi-device junction table (adapted from multi_device_migration.sql)
--   5. device_session_expiry_migration.sql
-- ===============================================================================


-- -------------------------------------------------------------------------------
-- STEP 1/5 - Base schema (admins, consumers, meter_readers, consumption, billing, payments, notifications, events, functions, triggers, RLS). Source: migration.sql
-- -------------------------------------------------------------------------------
-- 1. ADMINS TABLE
CREATE TABLE IF NOT EXISTS admins (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE admins ADD COLUMN IF NOT EXISTS role VARCHAR(20) NOT NULL DEFAULT 'admin';
ALTER TABLE admins DROP CONSTRAINT IF EXISTS admins_role_check;
ALTER TABLE admins ADD CONSTRAINT admins_role_check CHECK (role IN ('admin', 'super-admin'));

-- Bootstrap exactly one existing administrator when upgrading an installation
-- that does not have a Super Admin yet.
UPDATE admins
SET role = 'super-admin', updated_at = NOW()
WHERE id = (
    SELECT id FROM admins
    WHERE NOT EXISTS (SELECT 1 FROM admins WHERE role = 'super-admin')
    ORDER BY created_at ASC, id ASC
    LIMIT 1
);

-- 2. CONSUMERS TABLE
CREATE TABLE IF NOT EXISTS consumers (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    first_name VARCHAR(50) NOT NULL,
    middle_name VARCHAR(50),
    last_name VARCHAR(50) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    contact_number VARCHAR(20),
    purok_no INTEGER,
    password VARCHAR(255) NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- 3. METER READERS TABLE
CREATE TABLE IF NOT EXISTS meter_readers (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) NOT NULL,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- Account identities must remain unambiguous across every login role.
CREATE UNIQUE INDEX IF NOT EXISTS admins_username_ci_unique ON admins (lower(btrim(username)));
CREATE UNIQUE INDEX IF NOT EXISTS admins_email_ci_unique ON admins (lower(btrim(email)));
CREATE UNIQUE INDEX IF NOT EXISTS consumers_username_ci_unique ON consumers (lower(btrim(username)));
CREATE UNIQUE INDEX IF NOT EXISTS consumers_email_ci_unique ON consumers (lower(btrim(email)));
CREATE UNIQUE INDEX IF NOT EXISTS consumers_full_name_ci_unique
ON consumers (lower(regexp_replace(btrim(coalesce(first_name, '') || ' ' || coalesce(middle_name, '') || ' ' || coalesce(last_name, '')), '\s+', ' ', 'g')));
CREATE UNIQUE INDEX IF NOT EXISTS meter_readers_username_ci_unique ON meter_readers (lower(btrim(username)));
CREATE UNIQUE INDEX IF NOT EXISTS meter_readers_email_ci_unique ON meter_readers (lower(btrim(email)));

DO $$
DECLARE
    duplicate_username TEXT;
    duplicate_email TEXT;
BEGIN
    SELECT identity INTO duplicate_username
    FROM (
        SELECT lower(btrim(username)) AS identity FROM admins
        UNION ALL SELECT lower(btrim(username)) FROM consumers
        UNION ALL SELECT lower(btrim(username)) FROM meter_readers
    ) identities
    GROUP BY identity HAVING count(*) > 1 LIMIT 1;

    SELECT identity INTO duplicate_email
    FROM (
        SELECT lower(btrim(email)) AS identity FROM admins
        UNION ALL SELECT lower(btrim(email)) FROM consumers
        UNION ALL SELECT lower(btrim(email)) FROM meter_readers
    ) identities
    GROUP BY identity HAVING count(*) > 1 LIMIT 1;

    IF duplicate_username IS NOT NULL THEN
        RAISE EXCEPTION 'Resolve the existing duplicate username "%" before applying account uniqueness.', duplicate_username;
    END IF;
    IF duplicate_email IS NOT NULL THEN
        RAISE EXCEPTION 'Resolve the existing duplicate email "%" before applying account uniqueness.', duplicate_email;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION enforce_global_account_identity()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public
AS $$
DECLARE
    normalized_username TEXT := lower(btrim(NEW.username));
    normalized_email TEXT := lower(btrim(NEW.email));
    normalized_full_name TEXT;
    normalized_first_name TEXT;
    normalized_middle_name TEXT;
    normalized_last_name TEXT;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtext('account-username:' || normalized_username));
    PERFORM pg_advisory_xact_lock(hashtext('account-email:' || normalized_email));

    IF EXISTS (
        SELECT 1 FROM admins account WHERE lower(btrim(account.username)) = normalized_username AND (TG_TABLE_NAME <> 'admins' OR account.id <> NEW.id)
        UNION ALL
        SELECT 1 FROM consumers account WHERE lower(btrim(account.username)) = normalized_username AND (TG_TABLE_NAME <> 'consumers' OR account.id <> NEW.id)
        UNION ALL
        SELECT 1 FROM meter_readers account WHERE lower(btrim(account.username)) = normalized_username AND (TG_TABLE_NAME <> 'meter_readers' OR account.id <> NEW.id)
    ) THEN
        RAISE EXCEPTION 'The username % is already in use.', NEW.username USING ERRCODE = '23505';
    END IF;

    IF EXISTS (
        SELECT 1 FROM admins account WHERE lower(btrim(account.email)) = normalized_email AND (TG_TABLE_NAME <> 'admins' OR account.id <> NEW.id)
        UNION ALL
        SELECT 1 FROM consumers account WHERE lower(btrim(account.email)) = normalized_email AND (TG_TABLE_NAME <> 'consumers' OR account.id <> NEW.id)
        UNION ALL
        SELECT 1 FROM meter_readers account WHERE lower(btrim(account.email)) = normalized_email AND (TG_TABLE_NAME <> 'meter_readers' OR account.id <> NEW.id)
    ) THEN
        RAISE EXCEPTION 'The email address % is already in use.', NEW.email USING ERRCODE = '23505';
    END IF;

    IF TG_TABLE_NAME = 'consumers' THEN
        normalized_first_name := regexp_replace(btrim(NEW.first_name), '\s+', ' ', 'g');
        normalized_middle_name := NULLIF(regexp_replace(btrim(coalesce(NEW.middle_name, '')), '\s+', ' ', 'g'), '');
        normalized_last_name := regexp_replace(btrim(NEW.last_name), '\s+', ' ', 'g');
        normalized_full_name := lower(regexp_replace(btrim(normalized_first_name || ' ' || coalesce(normalized_middle_name, '') || ' ' || normalized_last_name), '\s+', ' ', 'g'));
        PERFORM pg_advisory_xact_lock(hashtext('consumer-name:' || normalized_full_name));
        IF EXISTS (
            SELECT 1 FROM consumers account
            WHERE lower(regexp_replace(btrim(coalesce(account.first_name, '') || ' ' || coalesce(account.middle_name, '') || ' ' || coalesce(account.last_name, '')), '\s+', ' ', 'g')) = normalized_full_name
              AND account.id <> NEW.id
        ) THEN
            RAISE EXCEPTION 'The resident full name % is already in use.', normalized_full_name USING ERRCODE = '23505';
        END IF;
    END IF;

    NEW.username := normalized_username;
    NEW.email := normalized_email;
    IF TG_TABLE_NAME = 'consumers' THEN
        NEW.first_name := normalized_first_name;
        NEW.middle_name := normalized_middle_name;
        NEW.last_name := normalized_last_name;
    END IF;
    RETURN NEW;
END;
$$;

REVOKE ALL ON FUNCTION enforce_global_account_identity() FROM PUBLIC, anon, authenticated;

DROP TRIGGER IF EXISTS trg_admin_account_identity ON admins;
CREATE TRIGGER trg_admin_account_identity BEFORE INSERT OR UPDATE OF username, email ON admins
FOR EACH ROW EXECUTE FUNCTION enforce_global_account_identity();
DROP TRIGGER IF EXISTS trg_consumer_account_identity ON consumers;
CREATE TRIGGER trg_consumer_account_identity BEFORE INSERT OR UPDATE OF username, email, first_name, middle_name, last_name ON consumers
FOR EACH ROW EXECUTE FUNCTION enforce_global_account_identity();
DROP TRIGGER IF EXISTS trg_meter_reader_account_identity ON meter_readers;
CREATE TRIGGER trg_meter_reader_account_identity BEFORE INSERT OR UPDATE OF username, email ON meter_readers
FOR EACH ROW EXECUTE FUNCTION enforce_global_account_identity();

-- Temporary login lockout shared by admins, consumers, and meter readers.
-- NOTE: lockout is enforced in JS (models/authModels.js) using
-- LOGIN_MAX_ATTEMPTS / LOGIN_LOCKOUT_MINUTES from backend/.env.
-- The old record_failed_login() DB function was intentionally removed so the
-- .env-driven JS path is the single source of truth.
ALTER TABLE admins ADD COLUMN IF NOT EXISTS failed_login_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE admins ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ;
ALTER TABLE consumers ADD COLUMN IF NOT EXISTS failed_login_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE consumers ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ;
ALTER TABLE meter_readers ADD COLUMN IF NOT EXISTS failed_login_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE meter_readers ADD COLUMN IF NOT EXISTS locked_until TIMESTAMPTZ;

-- 4. CONSUMPTION LOGS TABLE
CREATE TABLE IF NOT EXISTS consumption (
    id SERIAL PRIMARY KEY,
    consumer_id INTEGER NOT NULL REFERENCES consumers (id) ON DELETE CASCADE,
    reading_date DATE NOT NULL,
    previous_reading NUMERIC(10,2) NOT NULL DEFAULT 0.00,
    present_reading NUMERIC(10,2) NOT NULL,
    consumption NUMERIC(10,2) GENERATED ALWAYS AS (present_reading - previous_reading) STORED,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE consumption ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS idx_consumption_idempotency_key
ON consumption (idempotency_key)
WHERE idempotency_key IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_consumption_consumer_month
ON consumption (
    consumer_id,
    EXTRACT(YEAR FROM reading_date),
    EXTRACT(MONTH FROM reading_date)
);

-- 5. BILLING HISTORY TABLE
CREATE TABLE IF NOT EXISTS billing (
    id SERIAL PRIMARY KEY,
    consumption_id INTEGER NOT NULL REFERENCES consumption (id) ON DELETE CASCADE,
    user_id INTEGER NOT NULL REFERENCES consumers (id) ON DELETE CASCADE,
    billing_date DATE NOT NULL,
    due_date DATE NOT NULL,
    total_bill NUMERIC(10,2) NOT NULL,
    remaining_balance NUMERIC(10,2) NOT NULL DEFAULT 0.00,
    status VARCHAR(30) DEFAULT 'Unpaid',
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- CREATE PAYMENTS TABLE
CREATE TABLE IF NOT EXISTS payments (
    id SERIAL PRIMARY KEY,
    billing_id INTEGER NOT NULL REFERENCES billing (id) ON DELETE CASCADE,
    total_paid NUMERIC(10,2) NOT NULL,
    amount_tendered NUMERIC(10,2) NOT NULL,
    change_given NUMERIC(10,2) NOT NULL DEFAULT 0.00,
    remaining_balance NUMERIC(10,2) NOT NULL DEFAULT 0.00,
    payment_date DATE NOT NULL DEFAULT CURRENT_DATE,
    payment_method VARCHAR(40) NOT NULL DEFAULT 'Cash',
    reference_number VARCHAR(100),
    idempotency_key TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE payments ADD COLUMN IF NOT EXISTS payment_date DATE NOT NULL DEFAULT CURRENT_DATE;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS payment_method VARCHAR(40) NOT NULL DEFAULT 'Cash';
ALTER TABLE payments ADD COLUMN IF NOT EXISTS reference_number VARCHAR(100);
ALTER TABLE payments ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
ALTER TABLE payments ADD COLUMN IF NOT EXISTS amount_tendered NUMERIC(10,2);
ALTER TABLE payments ADD COLUMN IF NOT EXISTS change_given NUMERIC(10,2) NOT NULL DEFAULT 0.00;

-- Index for fast lookup queries when fetching payment logs by billing record
CREATE INDEX IF NOT EXISTS idx_payments_billing_id ON payments (billing_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_payments_idempotency_key
ON payments (idempotency_key)
WHERE idempotency_key IS NOT NULL;

-- Payment receipts are immutable financial history. Once a bill has any
-- payment, its source reading must be corrected through an audited adjustment
-- instead of silently recalculating the bill and invalidating the receipt.
CREATE OR REPLACE FUNCTION prevent_paid_consumption_mutation()
RETURNS TRIGGER AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM billing
        INNER JOIN payments ON payments.billing_id = billing.id
        WHERE billing.consumption_id = OLD.id
    ) THEN
        RAISE EXCEPTION
            'This meter reading cannot be changed because its bill already has a recorded payment.';
    END IF;

    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_prevent_paid_consumption_mutation ON consumption;
CREATE TRIGGER trg_prevent_paid_consumption_mutation
BEFORE UPDATE OR DELETE ON consumption
FOR EACH ROW
EXECUTE FUNCTION prevent_paid_consumption_mutation();

-- Persistent report metadata and immutable source-data snapshots.
CREATE TABLE IF NOT EXISTS generated_reports (
    id BIGSERIAL PRIMARY KEY,
    title VARCHAR(180) NOT NULL,
    report_type VARCHAR(30) NOT NULL CHECK (
        report_type IN ('consumption', 'billing', 'residents', 'analytics')
    ),
    start_date DATE NOT NULL,
    end_date DATE NOT NULL,
    sections TEXT[] NOT NULL DEFAULT ARRAY['summary']::TEXT[],
    generated_by INTEGER NOT NULL REFERENCES admins (id) ON DELETE RESTRICT,
    record_count INTEGER NOT NULL DEFAULT 0 CHECK (record_count >= 0),
    status VARCHAR(20) NOT NULL DEFAULT 'Ready' CHECK (status IN ('Ready', 'Failed')),
    report_data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Record a cumulative meter value and its billing as one transaction. The
-- initial previous value is accepted only when digitizing a consumer with no
-- earlier digital reading.
DROP FUNCTION IF EXISTS record_consumption_and_billing(INTEGER, DATE, NUMERIC, NUMERIC);
DROP FUNCTION IF EXISTS record_consumption_and_billing(INTEGER, DATE, NUMERIC, NUMERIC, TEXT);
CREATE OR REPLACE FUNCTION record_consumption_and_billing(
    p_consumer_id INTEGER,
    p_present_reading NUMERIC,
    p_initial_previous_reading NUMERIC,
    p_idempotency_key TEXT
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_consumer consumers%ROWTYPE;
    v_latest consumption%ROWTYPE;
    v_reading consumption%ROWTYPE;
    v_billing billing%ROWTYPE;
    v_previous NUMERIC(10,2);
    v_rate NUMERIC(12,2);
    v_total NUMERIC(10,2);
    v_reading_date DATE := (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila')::DATE;
BEGIN
    IF p_consumer_id IS NULL OR p_present_reading IS NULL OR NULLIF(TRIM(p_idempotency_key), '') IS NULL THEN
        RAISE EXCEPTION 'Consumer, present reading, and request key are required.';
    END IF;
    IF p_present_reading < 0 OR (p_initial_previous_reading IS NOT NULL AND p_initial_previous_reading < 0) THEN
        RAISE EXCEPTION 'Meter readings must be non-negative numbers.';
    END IF;

    -- Serialize readings for the same consumer, including simultaneous first records.
    PERFORM pg_advisory_xact_lock(p_consumer_id);

    SELECT * INTO v_consumer FROM consumers WHERE id = p_consumer_id;
    IF NOT FOUND THEN RAISE EXCEPTION 'Consumer account not found.' USING ERRCODE = '23503'; END IF;
    IF COALESCE(v_consumer.status, 'active') <> 'active' THEN
        RAISE EXCEPTION 'Readings cannot be recorded for an inactive consumer.';
    END IF;
    IF v_consumer.purok_no IS NULL THEN
        RAISE EXCEPTION 'Assign the consumer to a purok before recording a reading.';
    END IF;

    SELECT * INTO v_reading
    FROM consumption
    WHERE idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_reading.consumer_id <> p_consumer_id
           OR v_reading.reading_date <> v_reading_date
           OR v_reading.present_reading <> ROUND(p_present_reading, 2) THEN
            RAISE EXCEPTION 'This reading request key was already used for different details.';
        END IF;
        SELECT * INTO v_billing FROM billing WHERE consumption_id = v_reading.id;
        RETURN jsonb_build_object(
            'id', v_reading.id,
            'consumer_id', v_reading.consumer_id,
            'reading_date', v_reading.reading_date,
            'previous_reading', v_reading.previous_reading,
            'present_reading', v_reading.present_reading,
            'consumption', v_reading.consumption,
            'created_at', v_reading.created_at,
            'billing', to_jsonb(v_billing)
        );
    END IF;

    IF EXISTS (
        SELECT 1 FROM consumption
        WHERE consumer_id = p_consumer_id
          AND EXTRACT(YEAR FROM reading_date) = EXTRACT(YEAR FROM v_reading_date)
          AND EXTRACT(MONTH FROM reading_date) = EXTRACT(MONTH FROM v_reading_date)
    ) THEN
        RAISE EXCEPTION 'This consumer already has a consumption reading for the selected month.' USING ERRCODE = '23505';
    END IF;

    SELECT * INTO v_latest
    FROM consumption
    WHERE consumer_id = p_consumer_id
    ORDER BY reading_date DESC, id DESC
    LIMIT 1;

    IF FOUND THEN
        IF p_initial_previous_reading IS NOT NULL THEN
            RAISE EXCEPTION 'An initial previous reading may only be supplied for a consumer with no previous record.';
        END IF;
        IF v_reading_date <= v_latest.reading_date THEN
            RAISE EXCEPTION 'A reading has already been recorded for this consumer today or later.';
        END IF;
        v_previous := v_latest.present_reading;
    ELSE
        IF p_initial_previous_reading IS NULL THEN
            RAISE EXCEPTION 'Enter the previous logbook reading for this consumer''s first digital record.';
        END IF;
        v_previous := p_initial_previous_reading;
    END IF;

    IF p_present_reading < v_previous THEN
        RAISE EXCEPTION 'Present reading cannot be lower than the previous reading.';
    END IF;

    -- Requires system_settings_migration.sql before recording new readings.
    -- Use the configured rate only for this new bill; existing bills retain their totals.
    SELECT setting_value INTO v_rate
    FROM public.system_settings
    WHERE setting_key = 'rate_per_cubic_meter';

    IF v_rate IS NULL OR v_rate < 0 OR v_rate = 'NaN'::NUMERIC THEN
        RAISE EXCEPTION 'A valid non-negative water rate is required in system settings.';
    END IF;

    INSERT INTO consumption (consumer_id, reading_date, previous_reading, present_reading, idempotency_key)
    VALUES (p_consumer_id, v_reading_date, v_previous, p_present_reading, p_idempotency_key)
    RETURNING * INTO v_reading;

    v_total := ROUND(v_reading.consumption * v_rate, 2);
    INSERT INTO billing (
        consumption_id, user_id, billing_date, due_date, total_bill,
        remaining_balance, status
    ) VALUES (
        v_reading.id, p_consumer_id, v_reading_date,
        (date_trunc('month', v_reading_date) + INTERVAL '1 month - 1 day')::DATE,
        v_total, v_total, 'Unpaid'
    ) RETURNING * INTO v_billing;

    RETURN jsonb_build_object(
        'id', v_reading.id,
        'consumer_id', v_reading.consumer_id,
        'reading_date', v_reading.reading_date,
        'previous_reading', v_reading.previous_reading,
        'present_reading', v_reading.present_reading,
        'consumption', v_reading.consumption,
        'created_at', v_reading.created_at,
        'billing', to_jsonb(v_billing)
    );
END;
$$;

REVOKE ALL ON FUNCTION record_consumption_and_billing(INTEGER, NUMERIC, NUMERIC, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION record_consumption_and_billing(INTEGER, NUMERIC, NUMERIC, TEXT) FROM anon;
REVOKE ALL ON FUNCTION record_consumption_and_billing(INTEGER, NUMERIC, NUMERIC, TEXT) FROM authenticated;
GRANT EXECUTE ON FUNCTION record_consumption_and_billing(INTEGER, NUMERIC, NUMERIC, TEXT) TO service_role;

-- Existing installations receive the resident contact field without invalidating
-- legacy accounts. New accounts require it through backend validation.
ALTER TABLE consumers
ADD COLUMN IF NOT EXISTS contact_number VARCHAR(20);

CREATE INDEX IF NOT EXISTS idx_generated_reports_created_at
ON generated_reports (created_at DESC);

CREATE INDEX IF NOT EXISTS idx_generated_reports_generated_by
ON generated_reports (generated_by);

-- Records a payment and updates its billing balance in one database transaction.
-- The row lock prevents two admins from spending the same remaining balance.
DROP FUNCTION IF EXISTS record_payment_transaction(INTEGER, NUMERIC, DATE, TEXT, TEXT, TEXT);

CREATE OR REPLACE FUNCTION record_payment_transaction(
    p_billing_id INTEGER,
    p_amount NUMERIC,
    p_amount_tendered NUMERIC,
    p_payment_date DATE,
    p_payment_method TEXT,
    p_reference_number TEXT,
    p_idempotency_key TEXT
)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    billing_record billing%ROWTYPE;
    payment_record payments%ROWTYPE;
    updated_billing billing%ROWTYPE;
    next_balance NUMERIC(10,2);
    next_status VARCHAR(30);
BEGIN
    IF p_amount IS NULL OR p_amount <= 0 THEN
        RAISE EXCEPTION 'Payment amount must be greater than zero.';
    END IF;

    IF p_amount_tendered IS NULL OR p_amount_tendered <= 0 THEN
        RAISE EXCEPTION 'Amount received must be greater than zero.';
    END IF;

    IF p_payment_method NOT IN ('Cash', 'GCash') THEN
        RAISE EXCEPTION 'Unsupported payment method.';
    END IF;

    IF p_payment_date IS NULL THEN
        RAISE EXCEPTION 'Payment date is required.';
    END IF;

    IF p_payment_method <> 'Cash' AND NULLIF(TRIM(p_reference_number), '') IS NULL THEN
        RAISE EXCEPTION 'An electronic payment reference number is required.';
    END IF;

    IF p_payment_method = 'Cash' AND p_amount_tendered < p_amount THEN
        RAISE EXCEPTION 'Cash received cannot be lower than the amount applied.';
    END IF;

    IF p_payment_method <> 'Cash' AND ROUND(p_amount_tendered, 2) <> ROUND(p_amount, 2) THEN
        RAISE EXCEPTION 'Electronic payment must equal the amount applied to the bill.';
    END IF;

    IF p_idempotency_key IS NOT NULL THEN
        SELECT * INTO payment_record
        FROM payments
        WHERE idempotency_key = p_idempotency_key;

        IF FOUND THEN
            IF payment_record.billing_id <> p_billing_id
               OR payment_record.total_paid <> ROUND(p_amount, 2)
               OR payment_record.amount_tendered <> ROUND(p_amount_tendered, 2) THEN
                RAISE EXCEPTION 'This payment request key was already used for different payment details.';
            END IF;

            SELECT * INTO updated_billing
            FROM billing
            WHERE id = payment_record.billing_id;

            RETURN (to_jsonb(payment_record) - 'idempotency_key')
                || jsonb_build_object('billing', to_jsonb(updated_billing));
        END IF;
    END IF;

    SELECT * INTO billing_record
    FROM billing
    WHERE id = p_billing_id
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Billing record not found.';
    END IF;

    IF billing_record.remaining_balance <= 0 OR billing_record.status = 'Paid' THEN
        RAISE EXCEPTION 'This billing record is already fully paid.';
    END IF;

    IF billing_record.status = 'Partially Paid'
       AND p_amount < billing_record.remaining_balance THEN
        RAISE EXCEPTION 'This bill already has a partial payment. The next payment must settle the full remaining balance.';
    END IF;

    IF p_amount > billing_record.remaining_balance THEN
        RAISE EXCEPTION 'Payment amount cannot exceed the remaining balance.';
    END IF;

    next_balance := ROUND(billing_record.remaining_balance - p_amount, 2);
    next_status := CASE WHEN next_balance = 0 THEN 'Paid' ELSE 'Partially Paid' END;

    INSERT INTO payments (
        billing_id,
        total_paid,
        amount_tendered,
        change_given,
        remaining_balance,
        payment_date,
        payment_method,
        reference_number,
        idempotency_key
    )
    VALUES (
        p_billing_id,
        ROUND(p_amount, 2),
        ROUND(p_amount_tendered, 2),
        CASE
            WHEN p_payment_method = 'Cash'
                THEN ROUND(p_amount_tendered - p_amount, 2)
            ELSE 0.00
        END,
        next_balance,
        p_payment_date,
        p_payment_method,
        NULLIF(TRIM(p_reference_number), ''),
        NULLIF(TRIM(p_idempotency_key), '')
    )
    RETURNING * INTO payment_record;

    UPDATE billing
    SET
        remaining_balance = next_balance,
        status = next_status,
        updated_at = NOW()
    WHERE id = p_billing_id
    RETURNING * INTO updated_billing;

    INSERT INTO notifications (
        consumer_id,
        announcement_type,
        notification_type,
        priority,
        title,
        announcement_date,
        message,
        billing_id,
        payment_id,
        action_path,
        event_key
    ) VALUES (
        billing_record.user_id,
        'Payment Alert',
        'payment_received',
        CASE WHEN next_balance = 0 THEN 'normal' ELSE 'high' END,
        CASE WHEN next_balance = 0 THEN 'Payment received' ELSE 'Partial payment received' END,
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila')::DATE,
        CASE
            WHEN next_balance = 0 THEN format(
                'We received your payment of PHP %s for bill #%s. This bill is now fully paid.',
                ROUND(p_amount, 2), p_billing_id
            )
            ELSE format(
                'We received your payment of PHP %s for bill #%s. Remaining balance: PHP %s.',
                ROUND(p_amount, 2), p_billing_id, next_balance
            )
        END,
        p_billing_id,
        payment_record.id,
        '/consumer/billing-ledger',
        format('payment-received:%s', payment_record.id)
    ) ON CONFLICT (event_key) DO NOTHING;

    RETURN (to_jsonb(payment_record) - 'idempotency_key')
        || jsonb_build_object('billing', to_jsonb(updated_billing));
END;
$$;

REVOKE ALL ON FUNCTION record_payment_transaction(INTEGER, NUMERIC, NUMERIC, DATE, TEXT, TEXT, TEXT) FROM PUBLIC;
REVOKE ALL ON FUNCTION record_payment_transaction(INTEGER, NUMERIC, NUMERIC, DATE, TEXT, TEXT, TEXT) FROM anon;
REVOKE ALL ON FUNCTION record_payment_transaction(INTEGER, NUMERIC, NUMERIC, DATE, TEXT, TEXT, TEXT) FROM authenticated;
GRANT EXECUTE ON FUNCTION record_payment_transaction(INTEGER, NUMERIC, NUMERIC, DATE, TEXT, TEXT, TEXT) TO service_role;

-- 6. NOTIFICATIONS TABLE
-- Updated to allow a targeted consumer_id (NULL tracks system-wide admin announcements)
CREATE TABLE IF NOT EXISTS notifications (
    id SERIAL PRIMARY KEY,
    consumer_id INT REFERENCES consumers(id) ON DELETE CASCADE,
    announcement_type VARCHAR(50) NOT NULL,
    notification_type VARCHAR(50) NOT NULL DEFAULT 'announcement',
    priority VARCHAR(20) NOT NULL DEFAULT 'normal',
    title VARCHAR(255) NOT NULL,
    announcement_date DATE NOT NULL,
    message TEXT NOT NULL,
    billing_id INTEGER REFERENCES billing(id) ON DELETE SET NULL,
    consumption_id INTEGER REFERENCES consumption(id) ON DELETE SET NULL,
    payment_id INTEGER REFERENCES payments(id) ON DELETE SET NULL,
    action_path TEXT,
    event_key TEXT,
    created_at TIMESTAMPTZ DEFAULT now(),
    updated_at TIMESTAMPTZ DEFAULT now()
);

ALTER TABLE notifications
ADD COLUMN IF NOT EXISTS consumer_id INT REFERENCES consumers(id) ON DELETE CASCADE;
ALTER TABLE notifications ADD COLUMN IF NOT EXISTS notification_type VARCHAR(50) NOT NULL DEFAULT 'announcement';
ALTER TABLE notifications ADD COLUMN IF NOT EXISTS priority VARCHAR(20) NOT NULL DEFAULT 'normal';
ALTER TABLE notifications ADD COLUMN IF NOT EXISTS billing_id INTEGER REFERENCES billing(id) ON DELETE SET NULL;
ALTER TABLE notifications ADD COLUMN IF NOT EXISTS consumption_id INTEGER REFERENCES consumption(id) ON DELETE SET NULL;
ALTER TABLE notifications ADD COLUMN IF NOT EXISTS payment_id INTEGER REFERENCES payments(id) ON DELETE SET NULL;
ALTER TABLE notifications ADD COLUMN IF NOT EXISTS action_path TEXT;
ALTER TABLE notifications ADD COLUMN IF NOT EXISTS event_key TEXT;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conname = 'notifications_priority_check'
          AND conrelid = 'notifications'::regclass
    ) THEN
        ALTER TABLE notifications
        ADD CONSTRAINT notifications_priority_check
        CHECK (priority IN ('low', 'normal', 'high', 'critical'));
    END IF;
END;
$$;

-- Indexes for lightning-fast queries when filtering notifications by target consumer
CREATE INDEX IF NOT EXISTS notifications_consumer_id_idx
ON notifications (consumer_id);
CREATE INDEX IF NOT EXISTS notifications_billing_id_idx ON notifications (billing_id);
CREATE INDEX IF NOT EXISTS notifications_type_date_idx
ON notifications (notification_type, announcement_date DESC);
CREATE UNIQUE INDEX IF NOT EXISTS notifications_event_key_idx
ON notifications (event_key);

-- 7. NOTIFICATION READS (Junction table to handle isolated read/unread telemetry)
CREATE TABLE IF NOT EXISTS notification_reads (
    notification_id INTEGER NOT NULL REFERENCES notifications(id) ON DELETE CASCADE,
    consumer_id INT NOT NULL REFERENCES consumers(id) ON DELETE CASCADE,
    read_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    dismissed_at TIMESTAMPTZ,
    PRIMARY KEY (notification_id, consumer_id)
);

ALTER TABLE notification_reads
ADD COLUMN IF NOT EXISTS dismissed_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS notification_reads_consumer_id_idx
ON notification_reads (consumer_id);

-- Browser/device subscriptions used only for Web Push delivery. These records
-- never authenticate a consumer and are intentionally separate from
-- admin_trusted_devices.
CREATE TABLE IF NOT EXISTS consumer_push_subscriptions (
    id BIGSERIAL PRIMARY KEY,
    consumer_id INTEGER NOT NULL REFERENCES consumers(id) ON DELETE CASCADE,
    endpoint TEXT NOT NULL,
    endpoint_hash CHAR(64) NOT NULL UNIQUE,
    p256dh TEXT NOT NULL,
    auth_key TEXT NOT NULL,
    user_agent VARCHAR(500),
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revoked_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_consumer_push_subscriptions_consumer
ON consumer_push_subscriptions (consumer_id, enabled)
WHERE revoked_at IS NULL;

-- Every notification source (Express and PostgreSQL triggers alike) enters the
-- same delivery queue. The backend worker removes successful jobs and retries
-- transient push-service failures.
CREATE TABLE IF NOT EXISTS notification_push_jobs (
    notification_id INTEGER PRIMARY KEY REFERENCES notifications(id) ON DELETE CASCADE,
    attempts INTEGER NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_notification_push_jobs_available
ON notification_push_jobs (available_at, created_at);

CREATE OR REPLACE FUNCTION enqueue_notification_push()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO notification_push_jobs (notification_id)
    VALUES (NEW.id)
    ON CONFLICT (notification_id) DO UPDATE SET
        attempts = 0,
        available_at = now(),
        last_error = NULL;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

REVOKE ALL ON FUNCTION enqueue_notification_push() FROM PUBLIC, anon, authenticated;

DROP TRIGGER IF EXISTS trg_enqueue_notification_push ON notifications;
CREATE TRIGGER trg_enqueue_notification_push
AFTER INSERT OR UPDATE OF title, message, priority, action_path ON notifications
FOR EACH ROW
EXECUTE FUNCTION enqueue_notification_push();

-- Generate the notice from the completed billing row so its amount, due date,
-- and related IDs always match the consumer's actual bill.
CREATE OR REPLACE FUNCTION generate_billing_notification()
RETURNS TRIGGER AS $$
DECLARE
    consumer_name VARCHAR(100);
    reading_record consumption%ROWTYPE;
BEGIN
    SELECT regexp_replace(btrim(coalesce(first_name, '') || ' ' || coalesce(middle_name, '') || ' ' || coalesce(last_name, '')), '\s+', ' ', 'g') INTO consumer_name
    FROM consumers
    WHERE id = NEW.user_id;

    SELECT * INTO reading_record
    FROM consumption
    WHERE id = NEW.consumption_id;

    INSERT INTO notifications (
        consumer_id,
        announcement_type,
        notification_type,
        priority,
        title,
        announcement_date,
        message,
        billing_id,
        consumption_id,
        action_path,
        event_key
    ) VALUES (
        NEW.user_id,
        'Billing Alert',
        'bill_generated',
        'high',
        'New water bill available',
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila')::DATE,
        format(
            'Hello %s, your meter reading for %s has been recorded. Consumption: %s cubic meters. Amount due: PHP %s. Due date: %s.',
            consumer_name,
            reading_record.reading_date,
            reading_record.consumption,
            NEW.total_bill,
            NEW.due_date
        ),
        NEW.id,
        NEW.consumption_id,
        '/consumer/billing-ledger',
        format('bill-generated:%s', NEW.id)
    ) ON CONFLICT (event_key) DO NOTHING;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 9. TRIGGER BINDING
DROP TRIGGER IF EXISTS trg_after_consumption_insert ON consumption;
DROP TRIGGER IF EXISTS trg_after_billing_insert ON billing;
DROP FUNCTION IF EXISTS generate_billing_announcement();

CREATE TRIGGER trg_after_billing_insert
AFTER INSERT ON billing
FOR EACH ROW
EXECUTE FUNCTION generate_billing_notification();

CREATE OR REPLACE FUNCTION generate_consumer_status_notification()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.status IS DISTINCT FROM OLD.status THEN
        INSERT INTO notifications (
            consumer_id,
            announcement_type,
            notification_type,
            priority,
            title,
            announcement_date,
            message,
            action_path,
            event_key
        ) VALUES (
            NEW.id,
            'Account Alert',
            'account_status_changed',
            CASE WHEN NEW.status = 'inactive' THEN 'critical' ELSE 'high' END,
            CASE WHEN NEW.status = 'inactive' THEN 'Account deactivated' ELSE 'Account activated' END,
            (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila')::DATE,
            CASE
                WHEN NEW.status = 'inactive'
                    THEN 'Your WaterWise account has been deactivated. Contact the water district office if you need assistance.'
                ELSE 'Your WaterWise account is active again. You may now sign in and use the consumer portal.'
            END,
            '/consumer/profile-details',
            format('account-status:%s:%s:%s', NEW.id, NEW.status, txid_current())
        );
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_after_consumer_status_update ON consumers;
CREATE TRIGGER trg_after_consumer_status_update
AFTER UPDATE OF status ON consumers
FOR EACH ROW
EXECUTE FUNCTION generate_consumer_status_notification();

-- New unified cache query.

-- 13. UNIFIED GEMINI AI CONSUMPTION CACHE
-- Prediction, anomaly, and recommendation results are stored together.
-- result_type separates cache keys that are shared by the three AI features.
CREATE TABLE IF NOT EXISTS ai_consumption_cache (
    id BIGSERIAL PRIMARY KEY,
    result_type VARCHAR(30) NOT NULL CHECK (
        result_type IN ('prediction', 'anomaly', 'recommendation')
    ),
    cache_key VARCHAR(255) NOT NULL,
    scope VARCHAR(30) NOT NULL CHECK (
        scope IN ('overall', 'all-puroks', 'purok')
    ),
    result_period VARCHAR(10) NOT NULL CHECK (
        result_period IN ('monthly', 'yearly')
    ),
    purok VARCHAR(100),
    result_payload JSONB NOT NULL,
    source_signature VARCHAR(255) NOT NULL,
    source_record_count INTEGER NOT NULL DEFAULT 0,
    latest_consumption_id INTEGER REFERENCES consumption(id) ON DELETE SET NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_ai_consumption_cache_type_key
        UNIQUE (result_type, cache_key)
);

CREATE INDEX IF NOT EXISTS idx_ai_consumption_cache_lookup
ON ai_consumption_cache (result_type, scope, result_period);

CREATE INDEX IF NOT EXISTS idx_ai_consumption_cache_latest_consumption
ON ai_consumption_cache (latest_consumption_id);

-- Trusted browsers can skip admin login OTP until their role-specific expiry.
-- Only a SHA-256 hash of the random browser token is stored.
CREATE TABLE IF NOT EXISTS admin_trusted_devices (
    id BIGSERIAL PRIMARY KEY,
    admin_id INTEGER NOT NULL REFERENCES admins(id) ON DELETE CASCADE,
    token_hash CHAR(64) NOT NULL UNIQUE,
    role VARCHAR(20) NOT NULL CHECK (role IN ('admin', 'super-admin')),
    user_agent VARCHAR(500),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_used_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL,
    revoked_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_admin_trusted_devices_admin
ON admin_trusted_devices (admin_id, expires_at)
WHERE revoked_at IS NULL;

-- Persistent password-reset OTP request throttling. The HMAC request key keeps
-- account email addresses out of this table while remaining stable per account.
CREATE TABLE IF NOT EXISTS password_reset_request_limits (
    request_key CHAR(64) PRIMARY KEY,
    request_timestamps TIMESTAMPTZ[] NOT NULL DEFAULT ARRAY[]::TIMESTAMPTZ[],
    locked_until TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION reserve_password_reset_otp_request(p_request_key TEXT)
RETURNS JSONB
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_now TIMESTAMPTZ := now();
    recent_requests TIMESTAMPTZ[];
    current_lock TIMESTAMPTZ;
    last_request TIMESTAMPTZ;
    retry_seconds INTEGER;
BEGIN
    IF p_request_key !~ '^[a-f0-9]{64}$' THEN
        RAISE EXCEPTION 'Invalid password reset request key';
    END IF;

    INSERT INTO password_reset_request_limits (request_key)
    VALUES (p_request_key)
    ON CONFLICT (request_key) DO NOTHING;

    SELECT
        COALESCE(
            ARRAY(
                SELECT request_time
                FROM unnest(request_timestamps) AS request_time
                WHERE request_time > v_now - INTERVAL '1 hour'
                ORDER BY request_time
            ),
            ARRAY[]::TIMESTAMPTZ[]
        ),
        locked_until
    INTO recent_requests, current_lock
    FROM password_reset_request_limits
    WHERE request_key = p_request_key
    FOR UPDATE;

    IF current_lock IS NOT NULL AND current_lock > v_now THEN
        retry_seconds := GREATEST(1, CEIL(EXTRACT(EPOCH FROM current_lock - v_now))::INTEGER);
        RETURN jsonb_build_object(
            'allowed', false,
            'retry_after_seconds', retry_seconds,
            'reason', 'hourly_lock',
            'locked_until', current_lock
        );
    END IF;

    IF cardinality(recent_requests) > 0 THEN
        last_request := recent_requests[cardinality(recent_requests)];
        IF last_request > v_now - INTERVAL '5 minutes' THEN
            retry_seconds := GREATEST(
                1,
                CEIL(EXTRACT(EPOCH FROM (last_request + INTERVAL '5 minutes') - v_now))::INTEGER
            );
            UPDATE password_reset_request_limits
            SET request_timestamps = recent_requests, locked_until = NULL, updated_at = v_now
            WHERE request_key = p_request_key;
            RETURN jsonb_build_object(
                'allowed', false,
                'retry_after_seconds', retry_seconds,
                'reason', 'cooldown'
            );
        END IF;
    END IF;

    IF cardinality(recent_requests) >= 5 THEN
        current_lock := v_now + INTERVAL '1 hour';
        UPDATE password_reset_request_limits
        SET request_timestamps = recent_requests, locked_until = current_lock, updated_at = v_now
        WHERE request_key = p_request_key;
        RETURN jsonb_build_object(
            'allowed', false,
            'retry_after_seconds', 3600,
            'reason', 'hourly_lock',
            'locked_until', current_lock
        );
    END IF;

    recent_requests := array_append(recent_requests, v_now);
    current_lock := CASE
        WHEN cardinality(recent_requests) >= 5 THEN v_now + INTERVAL '1 hour'
        ELSE NULL
    END;

    UPDATE password_reset_request_limits
    SET request_timestamps = recent_requests, locked_until = current_lock, updated_at = v_now
    WHERE request_key = p_request_key;

    RETURN jsonb_build_object(
        'allowed', true,
        'retry_after_seconds', 0,
        'request_count', cardinality(recent_requests),
        'locked_until', current_lock
    );
END;
$$;

REVOKE ALL ON TABLE password_reset_request_limits FROM anon, authenticated;
REVOKE ALL ON FUNCTION reserve_password_reset_otp_request(TEXT) FROM PUBLIC, anon, authenticated;
GRANT EXECUTE ON FUNCTION reserve_password_reset_otp_request(TEXT) TO service_role;

-- Persistent community events managed through the authenticated backend.
CREATE TABLE IF NOT EXISTS events (
    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    title VARCHAR(150) NOT NULL CHECK (char_length(btrim(title)) BETWEEN 1 AND 150),
    description TEXT NOT NULL CHECK (char_length(btrim(description)) BETWEEN 1 AND 2000),
    event_date DATE NOT NULL,
    event_time TIME WITHOUT TIME ZONE NOT NULL,
    location VARCHAR(200) NOT NULL CHECK (char_length(btrim(location)) BETWEEN 1 AND 200),
    tags TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],
    status VARCHAR(20) NOT NULL DEFAULT 'Scheduled'
        CHECK (status IN ('Scheduled', 'Cancelled')),
    created_by BIGINT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS events_schedule_idx
ON events (event_date, event_time);

ALTER TABLE events ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE events FROM anon, authenticated;
GRANT ALL ON TABLE events TO service_role;
GRANT USAGE, SELECT ON SEQUENCE events_id_seq TO service_role;

-- Keep one global consumer notification synchronized with every event.
CREATE OR REPLACE FUNCTION sync_event_notification()
RETURNS TRIGGER
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = public
AS $$
DECLARE
    v_notification_id INTEGER;
    notification_title TEXT;
    notification_message TEXT;
BEGIN
    IF TG_OP = 'DELETE' THEN
        DELETE FROM notifications
        WHERE event_key = format('community-event:%s', OLD.id);
        RETURN OLD;
    END IF;

    notification_title := CASE
        WHEN NEW.status = 'Cancelled' THEN format('Event cancelled: %s', NEW.title)
        WHEN TG_OP = 'UPDATE' THEN format('Event updated: %s', NEW.title)
        ELSE format('New community event: %s', NEW.title)
    END;

    notification_message := CASE
        WHEN NEW.status = 'Cancelled' THEN
            format(
                '%s scheduled for %s at %s in %s has been cancelled.',
                NEW.title,
                to_char(NEW.event_date, 'FMMonth FMDD, YYYY'),
                to_char(NEW.event_time, 'FMHH12:MI AM'),
                NEW.location
            )
        ELSE
            format(
                '%s is scheduled on %s at %s in %s. %s',
                NEW.title,
                to_char(NEW.event_date, 'FMMonth FMDD, YYYY'),
                to_char(NEW.event_time, 'FMHH12:MI AM'),
                NEW.location,
                NEW.description
            )
    END;

    INSERT INTO notifications (
        consumer_id,
        announcement_type,
        notification_type,
        priority,
        title,
        announcement_date,
        message,
        action_path,
        event_key
    ) VALUES (
        NULL,
        left(COALESCE(NEW.tags[1], 'Community Event'), 50),
        'announcement',
        CASE WHEN NEW.status = 'Cancelled' THEN 'high' ELSE 'normal' END,
        notification_title,
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila')::DATE,
        notification_message,
        '/consumer/announcements',
        format('community-event:%s', NEW.id)
    )
    ON CONFLICT (event_key) DO UPDATE SET
        announcement_type = EXCLUDED.announcement_type,
        notification_type = EXCLUDED.notification_type,
        priority = EXCLUDED.priority,
        title = EXCLUDED.title,
        announcement_date = EXCLUDED.announcement_date,
        message = EXCLUDED.message,
        action_path = EXCLUDED.action_path,
        updated_at = now()
    RETURNING id INTO v_notification_id;

    -- An updated or cancelled event should appear unread again.
    IF TG_OP = 'UPDATE' THEN
        DELETE FROM notification_reads AS reads
        WHERE reads.notification_id = v_notification_id;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sync_event_notification ON events;
CREATE TRIGGER trg_sync_event_notification
AFTER INSERT OR UPDATE OR DELETE ON events
FOR EACH ROW
EXECUTE FUNCTION sync_event_notification();

-- Create notifications for events that existed before this trigger was installed.
INSERT INTO notifications (
    consumer_id,
    announcement_type,
    notification_type,
    priority,
    title,
    announcement_date,
    message,
    action_path,
    event_key
)
SELECT
    NULL,
    left(COALESCE(event.tags[1], 'Community Event'), 50),
    'announcement',
    CASE WHEN event.status = 'Cancelled' THEN 'high' ELSE 'normal' END,
    CASE
        WHEN event.status = 'Cancelled' THEN format('Event cancelled: %s', event.title)
        ELSE format('New community event: %s', event.title)
    END,
    (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila')::DATE,
    CASE
        WHEN event.status = 'Cancelled' THEN
            format(
                '%s scheduled for %s at %s in %s has been cancelled.',
                event.title,
                to_char(event.event_date, 'FMMonth FMDD, YYYY'),
                to_char(event.event_time, 'FMHH12:MI AM'),
                event.location
            )
        ELSE
            format(
                '%s is scheduled on %s at %s in %s. %s',
                event.title,
                to_char(event.event_date, 'FMMonth FMDD, YYYY'),
                to_char(event.event_time, 'FMHH12:MI AM'),
                event.location,
                event.description
            )
    END,
    '/consumer/announcements',
    format('community-event:%s', event.id)
FROM events AS event
ON CONFLICT (event_key) DO NOTHING;

REVOKE ALL ON FUNCTION sync_event_notification() FROM PUBLIC, anon, authenticated;


BEGIN;

ALTER TABLE public.admins ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.consumers ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.meter_readers ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.consumption ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.billing ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.payments ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.generated_reports ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.notifications ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.notification_reads ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.consumer_push_subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.notification_push_jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.ai_consumption_cache ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.admin_trusted_devices ENABLE ROW LEVEL SECURITY;
ALTER TABLE public.password_reset_request_limits ENABLE ROW LEVEL SECURITY;

REVOKE ALL ON TABLE public.admins FROM anon, authenticated;
REVOKE ALL ON TABLE public.consumers FROM anon, authenticated;
REVOKE ALL ON TABLE public.meter_readers FROM anon, authenticated;
REVOKE ALL ON TABLE public.consumption FROM anon, authenticated;
REVOKE ALL ON TABLE public.billing FROM anon, authenticated;
REVOKE ALL ON TABLE public.payments FROM anon, authenticated;
REVOKE ALL ON TABLE public.generated_reports FROM anon, authenticated;
REVOKE ALL ON TABLE public.notifications FROM anon, authenticated;
REVOKE ALL ON TABLE public.notification_reads FROM anon, authenticated;
REVOKE ALL ON TABLE public.consumer_push_subscriptions FROM anon, authenticated;
REVOKE ALL ON TABLE public.notification_push_jobs FROM anon, authenticated;
REVOKE ALL ON TABLE public.ai_consumption_cache FROM anon, authenticated;
REVOKE ALL ON TABLE public.admin_trusted_devices FROM anon, authenticated;
REVOKE ALL ON TABLE public.password_reset_request_limits FROM anon, authenticated;

COMMIT;


-- -------------------------------------------------------------------------------
-- STEP 2/5 - system_settings table + default rate (required by record_consumption_and_billing). Source: system_settings_migration.sql
-- -------------------------------------------------------------------------------
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


-- -------------------------------------------------------------------------------
-- STEP 3/5 - Adds last_activity_at for 15-min inactivity expiry. Source: inactivity_migration.sql
-- -------------------------------------------------------------------------------
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


-- -------------------------------------------------------------------------------
-- STEP 4/5 - user_device_sessions junction table (sole source of truth for device sessions). Fresh-create + conditional legacy migrate. Adapted from: multi_device_migration.sql
-- -------------------------------------------------------------------------------
-- Multi-device support: user_device_sessions junction table allows multiple
-- concurrent sessions per user. This is the sole source of truth for device
-- sessions (no single-column fingerprint on account tables).
-- Adapted for fresh setup: creates the table directly. The legacy data
-- migration below runs ONLY if the old device_fingerprint_hash columns still
-- exist (upgrading installs); fresh installs skip it automatically.

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

-- Legacy upgrade path only: migrate old single-column data when present.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'admins'
      AND column_name = 'device_fingerprint_hash'
  ) THEN
    INSERT INTO user_device_sessions (user_id, role, fingerprint_hash)
    SELECT id, 'admin', device_fingerprint_hash FROM admins
    WHERE device_fingerprint_hash IS NOT NULL
    ON CONFLICT (user_id, role, fingerprint_hash) DO NOTHING;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'consumers'
      AND column_name = 'device_fingerprint_hash'
  ) THEN
    INSERT INTO user_device_sessions (user_id, role, fingerprint_hash)
    SELECT id, 'consumer', device_fingerprint_hash FROM consumers
    WHERE device_fingerprint_hash IS NOT NULL
    ON CONFLICT (user_id, role, fingerprint_hash) DO NOTHING;
  END IF;

  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = 'meter_readers'
      AND column_name = 'device_fingerprint_hash'
  ) THEN
    INSERT INTO user_device_sessions (user_id, role, fingerprint_hash)
    SELECT id, 'meter-reader', device_fingerprint_hash FROM meter_readers
    WHERE device_fingerprint_hash IS NOT NULL
    ON CONFLICT (user_id, role, fingerprint_hash) DO NOTHING;
  END IF;
END;
$$;

-- RLS: block direct browser access, backend uses service-role key
ALTER TABLE user_device_sessions ENABLE ROW LEVEL SECURITY;
REVOKE ALL ON TABLE user_device_sessions FROM anon, authenticated;


-- -------------------------------------------------------------------------------
-- STEP 5/5 - Adds expires_at to user_device_sessions + per-role backfill. Source: device_session_expiry_migration.sql
-- -------------------------------------------------------------------------------
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

