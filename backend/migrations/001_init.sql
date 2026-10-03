CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS companies (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS drivers (
    id TEXT PRIMARY KEY,
    username TEXT NOT NULL DEFAULT '',
    first_name TEXT NOT NULL DEFAULT '',
    last_name TEXT NOT NULL DEFAULT '',
    full_name TEXT NOT NULL,
    password_hash TEXT NOT NULL DEFAULT '',
    license_issue_state TEXT NOT NULL DEFAULT '',
    license_number TEXT NOT NULL DEFAULT '',
    company_id UUID REFERENCES companies(id) ON DELETE SET NULL,
    carrier TEXT NOT NULL DEFAULT 'NEKSUS',
    phone TEXT NOT NULL DEFAULT '',
    email TEXT NOT NULL DEFAULT '',
    home_terminal_timezone TEXT NOT NULL DEFAULT 'America/Chicago',
    truck_unit TEXT NOT NULL DEFAULT '',
    trailer_number TEXT NOT NULL DEFAULT '',
    shipping_document TEXT NOT NULL DEFAULT '',
    vehicle_type TEXT NOT NULL DEFAULT 'TRACTOR',
    active BOOLEAN NOT NULL DEFAULT TRUE,
    certified BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Safe upgrades for databases created by earlier NEKSUS backend versions.
ALTER TABLE drivers ADD COLUMN IF NOT EXISTS username TEXT NOT NULL DEFAULT '';
ALTER TABLE drivers ADD COLUMN IF NOT EXISTS first_name TEXT NOT NULL DEFAULT '';
ALTER TABLE drivers ADD COLUMN IF NOT EXISTS last_name TEXT NOT NULL DEFAULT '';
ALTER TABLE drivers ADD COLUMN IF NOT EXISTS password_hash TEXT NOT NULL DEFAULT '';
ALTER TABLE drivers ADD COLUMN IF NOT EXISTS license_issue_state TEXT NOT NULL DEFAULT '';
ALTER TABLE drivers ADD COLUMN IF NOT EXISTS license_number TEXT NOT NULL DEFAULT '';

CREATE UNIQUE INDEX IF NOT EXISTS drivers_username_unique_idx
    ON drivers ((lower(username))) WHERE username <> '';

CREATE TABLE IF NOT EXISTS driver_sessions (
    token_hash BYTEA PRIMARY KEY,
    driver_id TEXT NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
    expires_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS driver_sessions_driver_idx ON driver_sessions(driver_id);
CREATE INDEX IF NOT EXISTS driver_sessions_expiry_idx ON driver_sessions(expires_at);

CREATE TABLE IF NOT EXISTS driver_live_state (
    driver_id TEXT PRIMARY KEY REFERENCES drivers(id) ON DELETE CASCADE,
    duty_status TEXT NOT NULL DEFAULT 'OFF' CHECK (duty_status IN ('OFF','SB','DR','ON')),
    connected BOOLEAN NOT NULL DEFAULT FALSE,
    location_text TEXT NOT NULL DEFAULT '',
    latitude DOUBLE PRECISION NOT NULL DEFAULT 0,
    longitude DOUBLE PRECISION NOT NULL DEFAULT 0,
    status_since TIMESTAMPTZ,
    revision BIGINT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS driver_hos_current (
    driver_id TEXT PRIMARY KEY REFERENCES drivers(id) ON DELETE CASCADE,
    break_remaining_seconds INTEGER NOT NULL DEFAULT 0,
    drive_remaining_seconds INTEGER NOT NULL DEFAULT 0,
    shift_remaining_seconds INTEGER NOT NULL DEFAULT 0,
    cycle_remaining_seconds INTEGER NOT NULL DEFAULT 0,
    shift_started_at TIMESTAMPTZ,
    last_10h_reset_at TIMESTAMPTZ,
    last_34h_reset_at TIMESTAMPTZ,
    calculated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    revision BIGINT NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS duty_segments (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id TEXT NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
    log_date DATE NOT NULL,
    start_minute INTEGER NOT NULL CHECK (start_minute BETWEEN 0 AND 1440),
    end_minute INTEGER CHECK (end_minute BETWEEN 0 AND 1440),
    duty_status TEXT NOT NULL CHECK (duty_status IN ('OFF','SB','DR','ON')),
    special_status TEXT NOT NULL DEFAULT '',
    note TEXT NOT NULL DEFAULT '',
    origin TEXT NOT NULL DEFAULT 'ELD',
    edited BOOLEAN NOT NULL DEFAULT FALSE,
    revision BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (end_minute IS NULL OR end_minute >= start_minute)
);

CREATE INDEX IF NOT EXISTS duty_segments_driver_date_idx
    ON duty_segments(driver_id, log_date, start_minute);

CREATE TABLE IF NOT EXISTS eld_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id TEXT NOT NULL REFERENCES drivers(id) ON DELETE CASCADE,
    log_date DATE NOT NULL,
    minute INTEGER NOT NULL CHECK (minute BETWEEN 0 AND 1440),
    event_time TIMESTAMPTZ,
    event_type TEXT NOT NULL DEFAULT 'duty_status',
    duty_status TEXT CHECK (duty_status IS NULL OR duty_status IN ('OFF','SB','DR','ON')),
    note TEXT NOT NULL DEFAULT '',
    location_text TEXT NOT NULL DEFAULT '',
    odometer_miles DOUBLE PRECISION NOT NULL DEFAULT 0,
    engine_hours DOUBLE PRECISION NOT NULL DEFAULT 0,
    duration_seconds INTEGER,
    origin TEXT NOT NULL DEFAULT 'ELD',
    diagnostic BOOLEAN NOT NULL DEFAULT FALSE,
    violation BOOLEAN NOT NULL DEFAULT FALSE,
    edited BOOLEAN NOT NULL DEFAULT FALSE,
    edit_reason TEXT NOT NULL DEFAULT '',
    co_driver TEXT NOT NULL DEFAULT '',
    revision BIGINT NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS eld_events_driver_date_idx
    ON eld_events(driver_id, log_date, minute);

CREATE TABLE IF NOT EXISTS alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    driver_id TEXT REFERENCES drivers(id) ON DELETE CASCADE,
    kind TEXT NOT NULL DEFAULT 'alert',
    priority TEXT NOT NULL DEFAULT 'attention',
    title TEXT NOT NULL,
    detail TEXT NOT NULL DEFAULT '',
    event_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'open',
    owner TEXT NOT NULL DEFAULT '',
    resolution TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS alerts_status_idx ON alerts(status, created_at DESC);
CREATE INDEX IF NOT EXISTS alerts_driver_idx ON alerts(driver_id, status);

CREATE OR REPLACE FUNCTION neksus_touch_updated_at()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_companies_touch ON companies;
CREATE TRIGGER trg_companies_touch BEFORE UPDATE ON companies
FOR EACH ROW EXECUTE FUNCTION neksus_touch_updated_at();

DROP TRIGGER IF EXISTS trg_drivers_touch ON drivers;
CREATE TRIGGER trg_drivers_touch BEFORE UPDATE ON drivers
FOR EACH ROW EXECUTE FUNCTION neksus_touch_updated_at();

DROP TRIGGER IF EXISTS trg_live_touch ON driver_live_state;
CREATE TRIGGER trg_live_touch BEFORE UPDATE ON driver_live_state
FOR EACH ROW EXECUTE FUNCTION neksus_touch_updated_at();

DROP TRIGGER IF EXISTS trg_segments_touch ON duty_segments;
CREATE TRIGGER trg_segments_touch BEFORE UPDATE ON duty_segments
FOR EACH ROW EXECUTE FUNCTION neksus_touch_updated_at();

DROP TRIGGER IF EXISTS trg_alerts_touch ON alerts;
CREATE TRIGGER trg_alerts_touch BEFORE UPDATE ON alerts
FOR EACH ROW EXECUTE FUNCTION neksus_touch_updated_at();

CREATE OR REPLACE FUNCTION neksus_notify_change()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    row_new JSONB := COALESCE(to_jsonb(NEW), '{}'::jsonb);
    row_old JSONB := COALESCE(to_jsonb(OLD), '{}'::jsonb);
    driver_value TEXT;
    id_value TEXT;
BEGIN
    driver_value := COALESCE(row_new->>'driver_id', row_new->>'id', row_old->>'driver_id', row_old->>'id', '');
    id_value := COALESCE(row_new->>'id', row_old->>'id', '');
    PERFORM pg_notify(
        'neksus_changes',
        json_build_object(
            'table', TG_TABLE_NAME,
            'op', TG_OP,
            'driver_id', driver_value,
            'id', id_value
        )::text
    );
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END;
$$;

DO $$
DECLARE t TEXT;
BEGIN
    FOREACH t IN ARRAY ARRAY['drivers','driver_live_state','driver_hos_current','duty_segments','eld_events','alerts']
    LOOP
        EXECUTE format('DROP TRIGGER IF EXISTS %I ON %I', 'trg_notify_' || t, t);
        EXECUTE format('CREATE TRIGGER %I AFTER INSERT OR UPDATE OR DELETE ON %I FOR EACH ROW EXECUTE FUNCTION neksus_notify_change()', 'trg_notify_' || t, t);
    END LOOP;
END;
$$;
