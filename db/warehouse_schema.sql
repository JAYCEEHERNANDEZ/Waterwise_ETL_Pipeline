-- =============================================================================
-- WaterWise Warehouse schema (ETL Stage 3: Load)
-- =============================================================================
-- Target : the WAREHOUSE Supabase project (the second project).
--          Do NOT run this on the main WaterWise application database.
-- Spec   : docs/load.md Section 2 (target tables), Section 6 (load_log)
--          docs/transformation.md Section 6.1 (column contract)
--
-- How to run:
--   Supabase dashboard of the warehouse project -> SQL Editor -> paste -> Run.
--   The script is safe to run more than once.
--
-- After the first run, give the two roles a password (never commit them):
--   ALTER ROLE etl_loader       WITH PASSWORD '<choose a strong password>';
--   ALTER ROLE warehouse_reader WITH PASSWORD '<choose a strong password>';
-- Then set WAREHOUSE_DB_URL in pipeline/.env to the session pooler string,
-- using the user name  etl_loader.<project-ref>
-- =============================================================================

BEGIN;

CREATE SCHEMA IF NOT EXISTS warehouse;

-- -----------------------------------------------------------------------------
-- Dimensions
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS warehouse.dim_purok (
    purok_key      CHAR(16)     PRIMARY KEY,
    purok_no       INTEGER      NOT NULL UNIQUE,          -- match key; 0 = UNKNOWN
    purok_label    VARCHAR(50)  NOT NULL,
    is_unknown     BOOLEAN      NOT NULL,
    -- added by the load stage
    load_batch_id  VARCHAR(64)  NOT NULL,
    loaded_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS warehouse.dim_consumer (
    consumer_key           CHAR(16)      PRIMARY KEY,
    consumer_id            INTEGER       NOT NULL UNIQUE,  -- match key
    username               VARCHAR(50)   NOT NULL,
    first_name             VARCHAR(50)   NOT NULL,
    middle_name            VARCHAR(50),
    last_name              VARCHAR(50)   NOT NULL,
    full_name              VARCHAR(152)  NOT NULL,
    email                  VARCHAR(255)  NOT NULL,
    contact_number         VARCHAR(20),
    purok_key              CHAR(16)      NOT NULL
                           REFERENCES warehouse.dim_purok (purok_key),
    purok_no               INTEGER       NOT NULL,
    purok_is_unknown       BOOLEAN       NOT NULL,
    status                 VARCHAR(10)   NOT NULL
                           CHECK (status IN ('ACTIVE', 'INACTIVE', 'UNKNOWN')),
    code_unmapped          BOOLEAN       NOT NULL,
    active_flag            BOOLEAN       NOT NULL,
    valid_from             TIMESTAMPTZ   NOT NULL,
    valid_to               TIMESTAMPTZ,
    is_current             BOOLEAN       NOT NULL,
    created_at             TIMESTAMPTZ   NOT NULL,
    updated_at             TIMESTAMPTZ   NOT NULL,
    transformation_run_id  VARCHAR(64)   NOT NULL,
    -- added by the load stage
    load_batch_id          VARCHAR(64)   NOT NULL,
    loaded_at              TIMESTAMPTZ   NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- Facts
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS warehouse.fact_consumption (
    consumption_key        CHAR(16)       PRIMARY KEY,
    consumption_id         INTEGER        NOT NULL UNIQUE,  -- match key
    consumer_key           CHAR(16)       NOT NULL
                           REFERENCES warehouse.dim_consumer (consumer_key),
    reading_date           DATE           NOT NULL,
    reading_year           SMALLINT       NOT NULL,
    reading_month          SMALLINT       NOT NULL,
    billing_period         CHAR(7)        NOT NULL,         -- YYYY-MM
    previous_reading       NUMERIC(12,2)  NOT NULL,
    present_reading        NUMERIC(12,2)  NOT NULL,
    consumption            NUMERIC(12,2)  NOT NULL CHECK (consumption >= 0),
    consumption_cbm        NUMERIC(12,2)  NOT NULL,
    days_in_period         INTEGER        NOT NULL,
    consumption_per_day    NUMERIC(12,4)  NOT NULL,
    reading_date_adjusted  BOOLEAN        NOT NULL,
    transformation_run_id  VARCHAR(64)    NOT NULL,
    -- added by the load stage
    load_batch_id          VARCHAR(64)    NOT NULL,
    loaded_at              TIMESTAMPTZ    NOT NULL DEFAULT now(),
    CONSTRAINT fact_consumption_reading_order
        CHECK (present_reading >= previous_reading)
);

CREATE TABLE IF NOT EXISTS warehouse.fact_billing (
    billing_key             CHAR(16)       PRIMARY KEY,
    billing_id              INTEGER        NOT NULL UNIQUE,  -- match key
    consumption_key         CHAR(16)       NOT NULL
                            REFERENCES warehouse.fact_consumption (consumption_key),
    consumer_key            CHAR(16)       NOT NULL
                            REFERENCES warehouse.dim_consumer (consumer_key),
    billing_date            DATE           NOT NULL,
    due_date                DATE           NOT NULL,
    billing_year            SMALLINT       NOT NULL,
    billing_month           SMALLINT       NOT NULL,
    billing_period          CHAR(7)        NOT NULL,         -- YYYY-MM
    total_bill              NUMERIC(12,2)  NOT NULL CHECK (total_bill >= 0),
    remaining_balance       NUMERIC(12,2)  NOT NULL CHECK (remaining_balance >= 0),
    status                  VARCHAR(10)    NOT NULL
                            CHECK (status IN ('PAID', 'PARTIAL', 'UNPAID', 'UNKNOWN')),
    status_recomputed       BOOLEAN        NOT NULL,
    balance_corrected       BOOLEAN        NOT NULL,
    due_date_corrected      BOOLEAN        NOT NULL,
    is_revenue_recognized   BOOLEAN        NOT NULL,
    days_to_due             INTEGER        NOT NULL,
    days_overdue            INTEGER        NOT NULL,
    is_overdue              BOOLEAN        NOT NULL,
    overdue_bucket          VARCHAR(10)    NOT NULL,
    outstanding_bill_count  INTEGER        NOT NULL,
    is_flagged_account      BOOLEAN        NOT NULL,
    transformation_run_id   VARCHAR(64)    NOT NULL,
    -- added by the load stage
    load_batch_id           VARCHAR(64)    NOT NULL,
    loaded_at               TIMESTAMPTZ    NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS warehouse.fact_payment (
    payment_key               CHAR(16)       PRIMARY KEY,
    payment_id                INTEGER        NOT NULL UNIQUE,  -- match key
    billing_key               CHAR(16)       NOT NULL
                              REFERENCES warehouse.fact_billing (billing_key),
    payment_date              DATE           NOT NULL,
    payment_year              SMALLINT       NOT NULL,
    payment_month             SMALLINT       NOT NULL,
    payment_method            VARCHAR(20)    NOT NULL
                              CHECK (payment_method IN ('CASH', 'GCASH', 'BANK_TRANSFER', 'OTHER')),
    code_unmapped             BOOLEAN        NOT NULL,
    total_paid                NUMERIC(12,2)  NOT NULL CHECK (total_paid >= 0),
    amount_tendered           NUMERIC(12,2)  NOT NULL,
    change_given              NUMERIC(12,2)  NOT NULL,
    remaining_balance         NUMERIC(12,2)  NOT NULL,
    reference_number          VARCHAR(100),
    payment_allocation_rank   INTEGER        NOT NULL,
    is_oldest_unpaid_payment  BOOLEAN        NOT NULL,
    collection_rate           NUMERIC(9,4),
    change_corrected          BOOLEAN        NOT NULL,
    transformation_run_id     VARCHAR(64)    NOT NULL,
    -- added by the load stage
    load_batch_id             VARCHAR(64)    NOT NULL,
    loaded_at                 TIMESTAMPTZ    NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- Load log (docs/load.md Section 6): one row per target table per load run.
-- Append-only: the load role can INSERT and SELECT, never UPDATE or DELETE.
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS warehouse.load_log (
    load_log_id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_id                 VARCHAR(64)  NOT NULL,
    transformation_run_id  VARCHAR(64),
    batch_id               VARCHAR(64)  NOT NULL,
    batch_window           VARCHAR(64),
    table_name             VARCHAR(40)  NOT NULL,
    load_type              VARCHAR(20)  NOT NULL DEFAULT 'INCREMENTAL',
    load_start_ts          TIMESTAMPTZ,
    load_end_ts            TIMESTAMPTZ,
    load_duration_sec      INTEGER,
    records_received       INTEGER      NOT NULL DEFAULT 0,
    records_inserted       INTEGER      NOT NULL DEFAULT 0,
    records_updated        INTEGER      NOT NULL DEFAULT 0,
    records_rejected       INTEGER      NOT NULL DEFAULT 0,
    status                 VARCHAR(20)  NOT NULL,
    pre_load_result        TEXT,
    post_load_result       TEXT,
    error_code             VARCHAR(60),
    error_message          TEXT,
    reviewed_by            VARCHAR(100),
    load_code_version      VARCHAR(20)  NOT NULL,
    logged_at              TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- -----------------------------------------------------------------------------
-- Indexes (primary keys and UNIQUE match keys are indexed automatically)
-- -----------------------------------------------------------------------------

CREATE INDEX IF NOT EXISTS idx_dim_consumer_purok
    ON warehouse.dim_consumer (purok_key);

CREATE INDEX IF NOT EXISTS idx_fact_consumption_consumer
    ON warehouse.fact_consumption (consumer_key);
CREATE INDEX IF NOT EXISTS idx_fact_consumption_period
    ON warehouse.fact_consumption (billing_period);
CREATE INDEX IF NOT EXISTS idx_fact_consumption_batch
    ON warehouse.fact_consumption (load_batch_id);

CREATE INDEX IF NOT EXISTS idx_fact_billing_consumer_status_due
    ON warehouse.fact_billing (consumer_key, status, due_date);
CREATE INDEX IF NOT EXISTS idx_fact_billing_consumption
    ON warehouse.fact_billing (consumption_key);
CREATE INDEX IF NOT EXISTS idx_fact_billing_period
    ON warehouse.fact_billing (billing_period);
CREATE INDEX IF NOT EXISTS idx_fact_billing_batch
    ON warehouse.fact_billing (load_batch_id);

CREATE INDEX IF NOT EXISTS idx_fact_payment_billing_date
    ON warehouse.fact_payment (billing_key, payment_date);
CREATE INDEX IF NOT EXISTS idx_fact_payment_batch
    ON warehouse.fact_payment (load_batch_id);

CREATE INDEX IF NOT EXISTS idx_load_log_batch_status
    ON warehouse.load_log (batch_id, status);
CREATE INDEX IF NOT EXISTS idx_load_log_run
    ON warehouse.load_log (run_id);

-- -----------------------------------------------------------------------------
-- "Data as of" view for dashboards: latest successful load per table
-- (docs/load.md Section 8, Completion and Handover)
-- -----------------------------------------------------------------------------

CREATE OR REPLACE VIEW warehouse.latest_successful_load AS
SELECT DISTINCT ON (table_name)
       table_name,
       batch_id,
       batch_window,
       run_id,
       transformation_run_id,
       load_end_ts,
       records_received,
       records_inserted,
       records_updated,
       records_rejected
FROM warehouse.load_log
WHERE status = 'SUCCESS'
ORDER BY table_name, load_end_ts DESC NULLS LAST, load_log_id DESC;

-- -----------------------------------------------------------------------------
-- Roles and privileges (docs/load.md Section 1, Limitations and Assumptions)
-- Roles are created without a password, so nobody can log in as them until
-- the project owner sets one (see the header of this file).
-- -----------------------------------------------------------------------------

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl_loader') THEN
        CREATE ROLE etl_loader LOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'warehouse_reader') THEN
        CREATE ROLE warehouse_reader LOGIN;
    END IF;
END
$$;

REVOKE ALL ON SCHEMA warehouse FROM PUBLIC;
REVOKE ALL ON ALL TABLES IN SCHEMA warehouse FROM PUBLIC;

-- Load role: write the five tables, append to the log. No DELETE, no DDL.
GRANT USAGE ON SCHEMA warehouse TO etl_loader;
GRANT SELECT, INSERT, UPDATE ON
    warehouse.dim_purok,
    warehouse.dim_consumer,
    warehouse.fact_consumption,
    warehouse.fact_billing,
    warehouse.fact_payment
TO etl_loader;
GRANT SELECT, INSERT ON warehouse.load_log TO etl_loader;
GRANT SELECT ON warehouse.latest_successful_load TO etl_loader;

-- Reporting role: read only.
GRANT USAGE ON SCHEMA warehouse TO warehouse_reader;
GRANT SELECT ON ALL TABLES IN SCHEMA warehouse TO warehouse_reader;

-- -----------------------------------------------------------------------------
-- Row Level Security
-- Supabase can turn RLS on for every new table. With RLS on and no policy, a
-- role that is not the owner sees no rows and cannot write, even with GRANTs.
-- RLS is enabled explicitly here, and each policy allows exactly what the
-- GRANTs above allow, so the GRANTs stay the single place that limits access
-- (for example, etl_loader still has no DELETE).
-- -----------------------------------------------------------------------------

DO $$
DECLARE
    t text;
BEGIN
    FOREACH t IN ARRAY ARRAY['dim_purok', 'dim_consumer', 'fact_consumption',
                             'fact_billing', 'fact_payment', 'load_log']
    LOOP
        EXECUTE format('ALTER TABLE warehouse.%I ENABLE ROW LEVEL SECURITY', t);

        EXECUTE format('DROP POLICY IF EXISTS etl_loader_select ON warehouse.%I', t);
        EXECUTE format('CREATE POLICY etl_loader_select ON warehouse.%I '
                       'FOR SELECT TO etl_loader USING (true)', t);

        EXECUTE format('DROP POLICY IF EXISTS etl_loader_insert ON warehouse.%I', t);
        EXECUTE format('CREATE POLICY etl_loader_insert ON warehouse.%I '
                       'FOR INSERT TO etl_loader WITH CHECK (true)', t);

        EXECUTE format('DROP POLICY IF EXISTS warehouse_reader_select ON warehouse.%I', t);
        EXECUTE format('CREATE POLICY warehouse_reader_select ON warehouse.%I '
                       'FOR SELECT TO warehouse_reader USING (true)', t);

        -- load_log is append-only: no UPDATE policy.
        EXECUTE format('DROP POLICY IF EXISTS etl_loader_update ON warehouse.%I', t);
        IF t <> 'load_log' THEN
            EXECUTE format('CREATE POLICY etl_loader_update ON warehouse.%I '
                           'FOR UPDATE TO etl_loader USING (true) WITH CHECK (true)', t);
        END IF;
    END LOOP;
END
$$;

COMMIT;
