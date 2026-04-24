                                                   ------ F2 OWNS POSTGRESQL DATABASE ------
-- Waste category metadata
CREATE TABLE waste_categories (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(50) UNIQUE NOT NULL,  -- food_waste, paper, etc
    avg_kg_per_litre DECIMAL(5,3) NOT NULL,
    colour_code     VARCHAR(7),
    description     TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- City zones
CREATE TABLE city_zones (
    id              SERIAL PRIMARY KEY,
    name            VARCHAR(100) NOT NULL,  -- Zone-1, North District, etc
    boundary_geojson JSONB,                 -- polygon of zone boundary
    collection_day   VARCHAR(20),           -- Monday, Tuesday, etc
    collection_time  TIME,                  -- 08:00, 14:00, etc
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Bin registry
CREATE TABLE bins (
    id              VARCHAR(20) PRIMARY KEY,   -- BIN-047
    zone_id         INTEGER REFERENCES city_zones(id),
    waste_category_id INTEGER REFERENCES waste_categories(id),
    volume_litres   DECIMAL(8,2) NOT NULL,     -- physical capacity
    lat             DECIMAL(10,7) NOT NULL,
    lng             DECIMAL(10,7) NOT NULL,
    address         TEXT,
    installed_at    TIMESTAMPTZ,
    last_maintained TIMESTAMPTZ,
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Current bin state (upserted by Flink on every reading)
CREATE TABLE bin_current_state (
    bin_id              VARCHAR(20) PRIMARY KEY REFERENCES bins(id),
    fill_level_pct      DECIMAL(5,2) NOT NULL,   -- 0.00 to 100.00
    estimated_weight_kg DECIMAL(8,2),             -- calculated field
    status              VARCHAR(20) NOT NULL,     -- normal, monitor, urgent, critical
    urgency_score       INTEGER,                  -- 0-100
    predicted_full_at   TIMESTAMPTZ,
    fill_rate_pct_per_hour DECIMAL(6,3),
    battery_level_pct   DECIMAL(5,2),
    last_reading_at     TIMESTAMPTZ NOT NULL,
    last_collected_at   TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ DEFAULT NOW()
);

-- Vehicle fleet
CREATE TABLE vehicles (
    id              VARCHAR(20) PRIMARY KEY,    -- LORRY-01
    registration    VARCHAR(20) UNIQUE NOT NULL,
    max_cargo_kg    DECIMAL(8,2) NOT NULL,       -- weight limit
    volume_m3       DECIMAL(6,2),
    waste_categories_supported VARCHAR[],        -- which waste types it accepts
    active          BOOLEAN DEFAULT TRUE,
    last_service_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Route plans (written by OR-Tools)
CREATE TABLE route_plans (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID,                        -- links to collection_jobs
    vehicle_id      VARCHAR(20) REFERENCES vehicles(id),
    route_type      VARCHAR(20) NOT NULL,        -- routine, emergency
    zone_id         INTEGER REFERENCES city_zones(id),
    waypoints       JSONB NOT NULL,              -- ordered array of bin stops
    total_bins      INTEGER,
    estimated_weight_kg DECIMAL(8,2),
    estimated_distance_km DECIMAL(8,2),
    estimated_minutes INTEGER,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    valid_for_date  DATE,
    status          VARCHAR(20) DEFAULT 'planned'  -- planned, active, completed
);

-- Zone analytics snapshots (written by Flink windowing)
CREATE TABLE zone_snapshots (
    id              BIGSERIAL PRIMARY KEY,
    zone_id         INTEGER REFERENCES city_zones(id),
    snapshot_at     TIMESTAMPTZ NOT NULL,
    avg_fill_level  DECIMAL(5,2),
    urgent_bin_count INTEGER,
    total_bins      INTEGER,
    dominant_waste_category VARCHAR(50),
    total_estimated_kg DECIMAL(10,2),
    window_minutes  INTEGER
);

-- ML model performance tracking
CREATE TABLE model_performance (
    id              BIGSERIAL PRIMARY KEY,
    model_version   VARCHAR(50) NOT NULL,
    trained_at      TIMESTAMPTZ NOT NULL,
    training_records INTEGER,
    mae_hours       DECIMAL(6,3),   -- mean absolute error in hours
    promoted_to_prod BOOLEAN DEFAULT FALSE,
    promoted_at     TIMESTAMPTZ
);


                                              ------ END OF DB SCHEMA ------

                                              ------ F3 OWNS DATABASE POSTGRESQL ------
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- Drivers
CREATE TABLE drivers (
    id              VARCHAR(20) PRIMARY KEY,    -- DRV-001
    name            VARCHAR(100) NOT NULL,
    phone           VARCHAR(20),
    keycloak_user_id VARCHAR(100) UNIQUE,       -- links to Keycloak
    zone_id         INTEGER REFERENCES city_zones(id),
    current_vehicle_id VARCHAR(20),
    status          VARCHAR(20) DEFAULT 'off_duty',  -- available, on_job, off_duty
    shift_start     TIME,
    shift_end       TIME,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Collection jobs (both routine and emergency)
CREATE TABLE collection_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type        VARCHAR(20) NOT NULL,        -- routine, emergency
    zone_id         INTEGER,
    state           VARCHAR(50) NOT NULL DEFAULT 'CREATED',
    priority        INTEGER DEFAULT 5,           -- 1=highest, 10=lowest

    -- For emergency jobs — the triggering bin
    trigger_bin_id  VARCHAR(20),
    trigger_urgency_score INTEGER,

    -- For routine jobs — the zone schedule
    scheduled_date  DATE,
    scheduled_time  TIME,
    schedule_id     UUID,                        -- links to routine_schedules

    -- Assignment
    assigned_vehicle_id  VARCHAR(20),
    assigned_driver_id   VARCHAR(20),
    route_plan_id        UUID,

    -- Weight tracking
    planned_weight_kg    DECIMAL(8,2),           -- from OR-Tools
    actual_weight_kg     DECIMAL(8,2),           -- from completion

    -- Timing
    created_at           TIMESTAMPTZ DEFAULT NOW(),
    confirmed_at         TIMESTAMPTZ,
    assigned_at          TIMESTAMPTZ,
    accepted_at          TIMESTAMPTZ,
    started_at           TIMESTAMPTZ,
    completed_at         TIMESTAMPTZ,
    cancelled_at         TIMESTAMPTZ,

    -- Failure tracking
    failure_reason       TEXT,
    retry_count          INTEGER DEFAULT 0,
    escalated_at         TIMESTAMPTZ,

    -- Audit
    kafka_offset         BIGINT,
    hyperledger_tx_id    VARCHAR(200)
);

-- Individual bin collections within a job
CREATE TABLE bin_collection_records (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID REFERENCES collection_jobs(id),
    bin_id          VARCHAR(20) NOT NULL,
    sequence_number INTEGER NOT NULL,            -- order in route
    planned_at      TIMESTAMPTZ,                 -- estimated arrival
    arrived_at      TIMESTAMPTZ,
    collected_at    TIMESTAMPTZ,                 -- driver tapped "Collected"
    skipped_at      TIMESTAMPTZ,                 -- driver marked as skip
    skip_reason     TEXT,                        -- bin locked, inaccessible, etc
    fill_level_at_collection DECIMAL(5,2),
    estimated_weight_kg DECIMAL(8,2),
    actual_weight_kg DECIMAL(8,2),               -- if weighed
    driver_notes    TEXT,
    photo_url       TEXT,                        -- optional photo evidence
    gps_lat         DECIMAL(10,7),
    gps_lng         DECIMAL(10,7)
);

-- State transition audit log
CREATE TABLE job_state_transitions (
    id              BIGSERIAL PRIMARY KEY,
    job_id          UUID REFERENCES collection_jobs(id),
    from_state      VARCHAR(50),
    to_state        VARCHAR(50) NOT NULL,
    reason          TEXT,
    actor           VARCHAR(100),                -- system, driver-id, supervisor-id
    metadata        JSONB,
    transitioned_at TIMESTAMPTZ DEFAULT NOW()
);

-- Step execution log
CREATE TABLE job_step_results (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id          UUID REFERENCES collection_jobs(id),
    step_name       VARCHAR(100) NOT NULL,
    attempt_number  INTEGER DEFAULT 1,
    success         BOOLEAN NOT NULL,
    request_payload JSONB,
    response_payload JSONB,
    duration_ms     INTEGER,
    executed_at     TIMESTAMPTZ DEFAULT NOW()
);

-- Routine collection schedules
CREATE TABLE routine_schedules (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    zone_id         INTEGER NOT NULL,
    waste_category_id INTEGER,                   -- null = all categories
    frequency       VARCHAR(20) NOT NULL,        -- weekly, daily, biweekly
    day_of_week     VARCHAR(20),                 -- Monday, etc
    time_of_day     TIME NOT NULL,
    active          BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Vehicle weight log per job
CREATE TABLE vehicle_weight_logs (
    id              BIGSERIAL PRIMARY KEY,
    job_id          UUID REFERENCES collection_jobs(id),
    vehicle_id      VARCHAR(20) NOT NULL,
    weight_before_kg DECIMAL(8,2),              -- tare weight at start
    weight_after_kg  DECIMAL(8,2),              -- gross weight at end
    net_cargo_kg     DECIMAL(8,2),              -- actual waste collected
    recorded_at      TIMESTAMPTZ DEFAULT NOW()
);