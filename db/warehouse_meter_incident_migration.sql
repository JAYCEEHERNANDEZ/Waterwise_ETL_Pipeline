-- Adds source meter-incident provenance to an existing WaterWise warehouse.
-- Safe to rerun. Existing facts remain ordinary actual readings.

BEGIN;

ALTER TABLE warehouse.fact_consumption
    ADD COLUMN IF NOT EXISTS reading_source VARCHAR(16) NOT NULL DEFAULT 'actual',
    ADD COLUMN IF NOT EXISTS carried_consumption NUMERIC(12,2) NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS prior_usage_basis VARCHAR(32),
    ADD COLUMN IF NOT EXISTS new_baseline NUMERIC(12,2),
    ADD COLUMN IF NOT EXISTS meter_incident_id INTEGER;

ALTER TABLE warehouse.fact_consumption
    DROP CONSTRAINT IF EXISTS fact_consumption_reading_source_check,
    DROP CONSTRAINT IF EXISTS fact_consumption_carried_non_negative,
    DROP CONSTRAINT IF EXISTS fact_consumption_reading_order,
    DROP CONSTRAINT IF EXISTS fact_consumption_incident_provenance;

ALTER TABLE warehouse.fact_consumption
    ADD CONSTRAINT fact_consumption_reading_source_check
        CHECK (reading_source IN ('actual', 'estimated', 'incident')),
    ADD CONSTRAINT fact_consumption_carried_non_negative
        CHECK (carried_consumption >= 0),
    ADD CONSTRAINT fact_consumption_reading_order
        CHECK (reading_source = 'incident' OR present_reading >= previous_reading),
    ADD CONSTRAINT fact_consumption_incident_provenance CHECK (
        reading_source <> 'incident' OR (
            meter_incident_id IS NOT NULL
            AND new_baseline IS NOT NULL
            AND present_reading >= new_baseline
            AND abs(consumption - (
                carried_consumption + present_reading - new_baseline
            )) <= 0.01
        )
    );

COMMIT;
