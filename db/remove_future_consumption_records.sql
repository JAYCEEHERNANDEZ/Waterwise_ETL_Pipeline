-- Removes consumption records dated later than today's official recording date.
-- WaterWise uses Asia/Manila for recording and billing dates.
--
-- IMPORTANT: Deleting a consumption row cascades to its billing and payments.
-- This script stores JSON backups of all affected rows before deletion.

CREATE TABLE IF NOT EXISTS consumption_cleanup_backups (
    id BIGSERIAL PRIMARY KEY,
    cleanup_batch TEXT NOT NULL,
    source_table TEXT NOT NULL CHECK (
        source_table IN ('consumption', 'billing', 'payments')
    ),
    source_id BIGINT NOT NULL,
    row_data JSONB NOT NULL,
    backed_up_at TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_consumption_cleanup_backups_batch
ON consumption_cleanup_backups (cleanup_batch, source_table);

-- Preview the records that the cleanup statement below will remove.
SELECT
    (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila')::DATE AS cutoff_date,
    consumption_record.id AS consumption_id,
    consumption_record.consumer_id,
    regexp_replace(btrim(coalesce(consumers.first_name, '') || ' ' || coalesce(consumers.middle_name, '') || ' ' || coalesce(consumers.last_name, '')), '\s+', ' ', 'g') AS consumer_name,
    consumption_record.reading_date,
    consumption_record.previous_reading,
    consumption_record.present_reading,
    consumption_record.consumption
FROM consumption AS consumption_record
LEFT JOIN consumers ON consumers.id = consumption_record.consumer_id
WHERE consumption_record.reading_date
    > (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila')::DATE
ORDER BY consumption_record.reading_date, consumption_record.consumer_id;

-- Back up and delete in one atomic statement. This does not rely on temporary
-- tables, so it works when the Supabase SQL editor commits statements separately.
WITH
cleanup_settings AS MATERIALIZED (
    SELECT
        (CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Manila')::DATE AS cutoff_date,
        TO_CHAR(CLOCK_TIMESTAMP(), 'YYYYMMDD-HH24MISS-MS')
            || '-'
            || TXID_CURRENT()::TEXT AS cleanup_batch
),
future_readings AS MATERIALIZED (
    SELECT
        consumption_record.id,
        settings.cutoff_date,
        settings.cleanup_batch
    FROM consumption AS consumption_record
    CROSS JOIN cleanup_settings AS settings
    WHERE consumption_record.reading_date > settings.cutoff_date
),
backed_up_payments AS (
    INSERT INTO consumption_cleanup_backups (
        cleanup_batch,
        source_table,
        source_id,
        row_data
    )
    SELECT
        targets.cleanup_batch,
        'payments',
        payment_record.id,
        TO_JSONB(payment_record)
    FROM future_readings AS targets
    JOIN billing AS billing_record
        ON billing_record.consumption_id = targets.id
    JOIN payments AS payment_record
        ON payment_record.billing_id = billing_record.id
    RETURNING source_id
),
backed_up_billings AS (
    INSERT INTO consumption_cleanup_backups (
        cleanup_batch,
        source_table,
        source_id,
        row_data
    )
    SELECT
        targets.cleanup_batch,
        'billing',
        billing_record.id,
        TO_JSONB(billing_record)
    FROM future_readings AS targets
    JOIN billing AS billing_record
        ON billing_record.consumption_id = targets.id
    -- Creates an execution dependency on payment backup completion.
    CROSS JOIN (SELECT COUNT(*) FROM backed_up_payments) AS payment_backup_status
    RETURNING source_id
),
backed_up_consumption AS (
    INSERT INTO consumption_cleanup_backups (
        cleanup_batch,
        source_table,
        source_id,
        row_data
    )
    SELECT
        targets.cleanup_batch,
        'consumption',
        consumption_record.id,
        TO_JSONB(consumption_record)
    FROM future_readings AS targets
    JOIN consumption AS consumption_record
        ON consumption_record.id = targets.id
    -- Creates an execution dependency on billing backup completion.
    CROSS JOIN (SELECT COUNT(*) FROM backed_up_billings) AS billing_backup_status
    RETURNING source_id
),
deleted_consumption AS (
    DELETE FROM consumption AS consumption_record
    WHERE consumption_record.id IN (
        SELECT source_id FROM backed_up_consumption
    )
    RETURNING consumption_record.id
)
SELECT
    settings.cleanup_batch,
    settings.cutoff_date,
    (SELECT COUNT(*) FROM deleted_consumption) AS deleted_consumption_records,
    (SELECT COUNT(*) FROM backed_up_billings) AS backed_up_billing_records,
    (SELECT COUNT(*) FROM backed_up_payments) AS backed_up_payment_records
FROM cleanup_settings AS settings;
