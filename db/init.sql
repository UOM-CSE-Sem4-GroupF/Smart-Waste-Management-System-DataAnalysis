------ F2 OWNS POSTGRESQL DATABASE ------
-- Waste category metadata
CREATE TABLE waste_categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    -- food_waste, paper, etc
    avg_kg_per_litre DECIMAL(5, 3) NOT NULL,
    colour_code VARCHAR(7),
    description TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
-- City zones
CREATE TABLE city_zones (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    -- Zone-1, North District, etc
    boundary_geojson JSONB,
    -- polygon of zone boundary
    collection_day VARCHAR(20),
    -- Monday, Tuesday, etc
    collection_time TIME,
    -- 08:00, 14:00, etc
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
-- Bin registry
CREATE TABLE bins (
    id VARCHAR(20) PRIMARY KEY,
    -- BIN-047
    zone_id INTEGER REFERENCES city_zones(id),
    waste_category_id INTEGER REFERENCES waste_categories(id),
    volume_litres DECIMAL(8, 2) NOT NULL,
    -- physical capacity
    lat DECIMAL(10, 7) NOT NULL,
    lng DECIMAL(10, 7) NOT NULL,
    address TEXT,
    installed_at TIMESTAMPTZ,
    last_maintained TIMESTAMPTZ,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
-- Current bin state (upserted by Flink on every reading)
CREATE TABLE bin_current_state (
    bin_id VARCHAR(20) PRIMARY KEY REFERENCES bins(id),
    fill_level_pct DECIMAL(5, 2) NOT NULL,
    -- 0.00 to 100.00
    estimated_weight_kg DECIMAL(8, 2),
    -- calculated field
    status VARCHAR(20) NOT NULL,
    -- normal, monitor, urgent, critical
    urgency_score INTEGER,
    -- 0-100
    predicted_full_at TIMESTAMPTZ,
    fill_rate_pct_per_hour DECIMAL(6, 3),
    battery_level_pct DECIMAL(5, 2),
    last_reading_at TIMESTAMPTZ NOT NULL,
    last_collected_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
-- Vehicle fleet
CREATE TABLE vehicles (
    id VARCHAR(20) PRIMARY KEY,
    -- LORRY-01
    registration VARCHAR(20) UNIQUE NOT NULL,
    max_cargo_kg DECIMAL(8, 2) NOT NULL,
    -- weight limit
    volume_m3 DECIMAL(6, 2),
    waste_categories_supported VARCHAR [],
    -- which waste types it accepts
    active BOOLEAN DEFAULT TRUE,
    last_service_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
-- Route plans (written by OR-Tools)
CREATE TABLE route_plans (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID,
    -- links to collection_jobs
    vehicle_id VARCHAR(20) REFERENCES vehicles(id),
    route_type VARCHAR(20) NOT NULL,
    -- routine, emergency
    zone_id INTEGER REFERENCES city_zones(id),
    waypoints JSONB NOT NULL,
    -- ordered array of bin stops
    total_bins INTEGER,
    estimated_weight_kg DECIMAL(8, 2),
    estimated_distance_km DECIMAL(8, 2),
    estimated_minutes INTEGER,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    valid_for_date DATE,
    status VARCHAR(20) DEFAULT 'planned' -- planned, active, completed
);
-- Zone analytics snapshots (written by Flink windowing)
CREATE TABLE zone_snapshots (
    id BIGSERIAL PRIMARY KEY,
    zone_id INTEGER REFERENCES city_zones(id),
    snapshot_at TIMESTAMPTZ NOT NULL,
    avg_fill_level DECIMAL(5, 2),
    urgent_bin_count INTEGER,
    total_bins INTEGER,
    dominant_waste_category VARCHAR(50),
    total_estimated_kg DECIMAL(10, 2),
    window_minutes INTEGER
);
-- ML model performance tracking
CREATE TABLE model_performance (
    id BIGSERIAL PRIMARY KEY,
    model_version VARCHAR(50) NOT NULL,
    trained_at TIMESTAMPTZ NOT NULL,
    training_records INTEGER,
    mae_hours DECIMAL(6, 3),
    -- mean absolute error in hours
    promoted_to_prod BOOLEAN DEFAULT FALSE,
    promoted_at TIMESTAMPTZ
);
------ END OF DB SCHEMA ------
------ F3 OWNS DATABASE POSTGRESQL ------
CREATE EXTENSION IF NOT EXISTS pgcrypto;
-- Drivers
CREATE TABLE drivers (
    id VARCHAR(20) PRIMARY KEY,
    -- DRV-001
    name VARCHAR(100) NOT NULL,
    phone VARCHAR(20),
    keycloak_user_id VARCHAR(100) UNIQUE,
    -- links to Keycloak
    zone_id INTEGER REFERENCES city_zones(id),
    current_vehicle_id VARCHAR(20),
    status VARCHAR(20) DEFAULT 'off_duty',
    -- available, on_job, off_duty
    shift_start TIME,
    shift_end TIME,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
-- Collection jobs (both routine and emergency)
CREATE TABLE collection_jobs (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_type VARCHAR(20) NOT NULL,
    -- routine, emergency
    zone_id INTEGER,
    state VARCHAR(50) NOT NULL DEFAULT 'CREATED',
    priority INTEGER DEFAULT 5,
    -- 1=highest, 10=lowest
    -- For emergency jobs — the triggering bin
    trigger_bin_id VARCHAR(20),
    trigger_urgency_score INTEGER,
    -- For routine jobs — the zone schedule
    scheduled_date DATE,
    scheduled_time TIME,
    schedule_id UUID,
    -- links to routine_schedules
    -- Assignment
    assigned_vehicle_id VARCHAR(20),
    assigned_driver_id VARCHAR(20),
    route_plan_id UUID,
    -- Weight tracking
    planned_weight_kg DECIMAL(8, 2),
    -- from OR-Tools
    actual_weight_kg DECIMAL(8, 2),
    -- from completion
    -- Timing
    created_at TIMESTAMPTZ DEFAULT NOW(),
    confirmed_at TIMESTAMPTZ,
    assigned_at TIMESTAMPTZ,
    accepted_at TIMESTAMPTZ,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    cancelled_at TIMESTAMPTZ,
    -- Failure tracking
    failure_reason TEXT,
    retry_count INTEGER DEFAULT 0,
    escalated_at TIMESTAMPTZ,
    -- Audit
    kafka_offset BIGINT,
    hyperledger_tx_id VARCHAR(200)
);
-- Individual bin collections within a job
CREATE TABLE bin_collection_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID REFERENCES collection_jobs(id),
    bin_id VARCHAR(20) NOT NULL,
    sequence_number INTEGER NOT NULL,
    -- order in route
    planned_at TIMESTAMPTZ,
    -- estimated arrival
    arrived_at TIMESTAMPTZ,
    collected_at TIMESTAMPTZ,
    -- driver tapped "Collected"
    skipped_at TIMESTAMPTZ,
    -- driver marked as skip
    skip_reason TEXT,
    -- bin locked, inaccessible, etc
    fill_level_at_collection DECIMAL(5, 2),
    estimated_weight_kg DECIMAL(8, 2),
    actual_weight_kg DECIMAL(8, 2),
    -- if weighed
    driver_notes TEXT,
    photo_url TEXT,
    -- optional photo evidence
    gps_lat DECIMAL(10, 7),
    gps_lng DECIMAL(10, 7)
);
-- State transition audit log
CREATE TABLE job_state_transitions (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID REFERENCES collection_jobs(id),
    from_state VARCHAR(50),
    to_state VARCHAR(50) NOT NULL,
    reason TEXT,
    actor VARCHAR(100),
    -- system, driver-id, supervisor-id
    metadata JSONB,
    transitioned_at TIMESTAMPTZ DEFAULT NOW()
);
-- Step execution log
CREATE TABLE job_step_results (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id UUID REFERENCES collection_jobs(id),
    step_name VARCHAR(100) NOT NULL,
    attempt_number INTEGER DEFAULT 1,
    success BOOLEAN NOT NULL,
    request_payload JSONB,
    response_payload JSONB,
    duration_ms INTEGER,
    executed_at TIMESTAMPTZ DEFAULT NOW()
);
-- Routine collection schedules
CREATE TABLE routine_schedules (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    zone_id INTEGER NOT NULL,
    waste_category_id INTEGER,
    -- null = all categories
    frequency VARCHAR(20) NOT NULL,
    -- weekly, daily, biweekly
    day_of_week VARCHAR(20),
    -- Monday, etc
    time_of_day TIME NOT NULL,
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
-- Vehicle weight log per job
CREATE TABLE vehicle_weight_logs (
    id BIGSERIAL PRIMARY KEY,
    job_id UUID REFERENCES collection_jobs(id),
    vehicle_id VARCHAR(20) NOT NULL,
    weight_before_kg DECIMAL(8, 2),
    -- tare weight at start
    weight_after_kg DECIMAL(8, 2),
    -- gross weight at end
    net_cargo_kg DECIMAL(8, 2),
    -- actual waste collected
    recorded_at TIMESTAMPTZ DEFAULT NOW()
);
------ SEED DATA FOR F2 ROUTE OPTIMIZER ------
-- Waste categories from the canonical spec (explicit IDs for deterministic references)
INSERT INTO waste_categories (
        id,
        name,
        avg_kg_per_litre,
        colour_code,
        description
    )
VALUES (
        1,
        'food_waste',
        0.900,
        '#8B4513',
        'Organic and food waste'
    ),
    (
        2,
        'paper',
        0.100,
        '#4169E1',
        'Paper and cardboard'
    ),
    (3, 'glass', 2.500, '#228B22', 'Glass waste'),
    (
        4,
        'plastic',
        0.050,
        '#FF6347',
        'Plastic waste'
    ),
    (
        5,
        'general',
        0.300,
        '#808080',
        'Mixed/general waste'
    ),
    (
        6,
        'e_waste',
        3.200,
        '#FFD700',
        'Electronic waste'
    ) ON CONFLICT (id) DO NOTHING;
-- Keep SERIAL counters in sync after explicit inserts
SELECT setval(
        'waste_categories_id_seq',
        (
            SELECT MAX(id)
            FROM waste_categories
        )
    );
INSERT INTO city_zones (
        id,
        name,
        collection_day,
        collection_time,
        active
    )
VALUES (1, 'Zone-1', 'Monday', '08:00', TRUE),
    (2, 'Zone-2', 'Tuesday', '08:00', TRUE),
    (3, 'Zone-3', 'Wednesday', '08:00', TRUE),
    (4, 'Zone-4', 'Thursday', '08:00', TRUE) ON CONFLICT (id) DO NOTHING;
SELECT setval(
        'city_zones_id_seq',
        (
            SELECT MAX(id)
            FROM city_zones
        )
    );
-- Bin IDs and category mapping aligned with Edge simulator payloads
INSERT INTO bins (
        id,
        zone_id,
        waste_category_id,
        volume_litres,
        lat,
        lng,
        address,
        active
    )
VALUES (
        'BIN-001',
        1,
        1,
        240.00,
        6.9270790,
        79.8612440,
        'Zone-1 Bin 001',
        TRUE
    ),
    (
        'BIN-002',
        1,
        5,
        240.00,
        6.9282100,
        79.8625100,
        'Zone-1 Bin 002',
        TRUE
    ),
    (
        'BIN-003',
        2,
        2,
        360.00,
        6.9311000,
        79.8641000,
        'Zone-2 Bin 003',
        TRUE
    ),
    (
        'BIN-004',
        2,
        4,
        240.00,
        6.9325000,
        79.8657000,
        'Zone-2 Bin 004',
        TRUE
    ),
    (
        'BIN-005',
        2,
        3,
        120.00,
        6.9340000,
        79.8672000,
        'Zone-2 Bin 005',
        TRUE
    ),
    (
        'BIN-006',
        3,
        1,
        240.00,
        6.9362000,
        79.8689000,
        'Zone-3 Bin 006',
        TRUE
    ),
    (
        'BIN-007',
        3,
        5,
        360.00,
        6.9381000,
        79.8701000,
        'Zone-3 Bin 007',
        TRUE
    ),
    (
        'BIN-008',
        3,
        4,
        240.00,
        6.9395000,
        79.8718000,
        'Zone-3 Bin 008',
        TRUE
    ),
    (
        'BIN-009',
        4,
        3,
        120.00,
        6.9410000,
        79.8732000,
        'Zone-4 Bin 009',
        TRUE
    ),
    (
        'BIN-010',
        4,
        5,
        240.00,
        6.9424000,
        79.8749000,
        'Zone-4 Bin 010',
        TRUE
    ) ON CONFLICT (id) DO NOTHING;
-- Vehicle pool for emergency routing experiments
INSERT INTO vehicles (
        id,
        registration,
        max_cargo_kg,
        volume_m3,
        waste_categories_supported,
        active
    )
VALUES (
        'LORRY-01',
        'WP-TRK-1001',
        1500.00,
        12.00,
        ARRAY ['food_waste','general','paper'],
        TRUE
    ),
    (
        'LORRY-02',
        'WP-TRK-1002',
        1200.00,
        10.00,
        ARRAY ['plastic','paper','general'],
        TRUE
    ),
    (
        'LORRY-03',
        'WP-TRK-1003',
        1000.00,
        8.00,
        ARRAY ['glass'],
        TRUE
    ),
    (
        'LORRY-04',
        'WP-TRK-1004',
        1800.00,
        14.00,
        ARRAY ['food_waste','general','plastic','paper'],
        TRUE
    ) ON CONFLICT (id) DO NOTHING;
-- Initial current-state rows aligned to seeded bins (including urgent/critical examples)
-- Values mirror live Kafka bridge payload style (payload.bin_id/fill_level_pct/battery_level_pct)
INSERT INTO bin_current_state (
        bin_id,
        fill_level_pct,
        estimated_weight_kg,
        status,
        urgency_score,
        predicted_full_at,
        fill_rate_pct_per_hour,
        battery_level_pct,
        last_reading_at,
        updated_at
    )
VALUES (
        'BIN-001',
        82.40,
        177.98,
        'urgent',
        81,
        NOW() + INTERVAL '2 hours',
        7.200,
        91.0,
        NOW() - INTERVAL '1 minute',
        NOW()
    ),
    (
        'BIN-002',
        61.20,
        44.06,
        'monitor',
        52,
        NOW() + INTERVAL '6 hours',
        3.100,
        88.5,
        NOW() - INTERVAL '2 minutes',
        NOW()
    ),
    (
        'BIN-003',
        46.30,
        16.67,
        'normal',
        28,
        NOW() + INTERVAL '18 hours',
        1.500,
        93.0,
        NOW() - INTERVAL '2 minutes',
        NOW()
    ),
    (
        'BIN-004',
        73.90,
        8.87,
        'monitor',
        59,
        NOW() + INTERVAL '4 hours',
        2.400,
        86.0,
        NOW() - INTERVAL '3 minutes',
        NOW()
    ),
    (
        'BIN-005',
        91.50,
        274.50,
        'critical',
        93,
        NOW() + INTERVAL '45 minutes',
        5.900,
        79.0,
        NOW() - INTERVAL '1 minute',
        NOW()
    ),
    (
        'BIN-006',
        67.80,
        146.45,
        'monitor',
        56,
        NOW() + INTERVAL '5 hours',
        3.700,
        90.0,
        NOW() - INTERVAL '1 minute',
        NOW()
    ),
    (
        'BIN-007',
        88.20,
        95.26,
        'urgent',
        84,
        NOW() + INTERVAL '90 minutes',
        4.600,
        87.0,
        NOW() - INTERVAL '2 minutes',
        NOW()
    ),
    (
        'BIN-008',
        72.68,
        8.72,
        'monitor',
        58,
        NOW() + INTERVAL '4 hours',
        2.200,
        85.4,
        NOW() - INTERVAL '1 minute',
        NOW()
    ),
    (
        'BIN-009',
        79.60,
        238.80,
        'urgent',
        78,
        NOW() + INTERVAL '3 hours',
        2.900,
        84.5,
        NOW() - INTERVAL '3 minutes',
        NOW()
    ),
    (
        'BIN-010',
        64.90,
        46.73,
        'monitor',
        54,
        NOW() + INTERVAL '6 hours',
        2.500,
        86.7,
        NOW() - INTERVAL '1 minute',
        NOW()
    ) ON CONFLICT (bin_id) DO
UPDATE
SET fill_level_pct = EXCLUDED.fill_level_pct,
    estimated_weight_kg = EXCLUDED.estimated_weight_kg,
    status = EXCLUDED.status,
    urgency_score = EXCLUDED.urgency_score,
    predicted_full_at = EXCLUDED.predicted_full_at,
    fill_rate_pct_per_hour = EXCLUDED.fill_rate_pct_per_hour,
    battery_level_pct = EXCLUDED.battery_level_pct,
    last_reading_at = EXCLUDED.last_reading_at,
    updated_at = EXCLUDED.updated_at;

-- Drivers
INSERT INTO drivers (id, name, phone, zone_id, current_vehicle_id, status, shift_start, shift_end)
VALUES
    ('DRV-001', 'Amith Perera', '0712345678', 1, 'LORRY-01', 'available', '08:00', '16:00'),
    ('DRV-002', 'Samantha Silva', '0712345679', 2, 'LORRY-02', 'on_job', '08:00', '16:00'),
    ('DRV-003', 'Rajesh Kumar', '0712345680', 3, 'LORRY-03', 'available', '14:00', '22:00'),
    ('DRV-004', 'Maria Gonzalez', '0712345681', 4, 'LORRY-04', 'off_duty', '08:00', '16:00'),
    ('DRV-005', 'Hassan Ahmed', '0712345682', 1, NULL, 'available', '22:00', '06:00')
ON CONFLICT (id) DO NOTHING;

-- Collection jobs
INSERT INTO collection_jobs (job_type, zone_id, state, priority, assigned_vehicle_id, assigned_driver_id, planned_weight_kg, route_plan_id, created_at, confirmed_at, assigned_at, started_at, completed_at)
VALUES
    ('routine', 1, 'COMPLETED', 5, 'LORRY-01', 'DRV-001', 268.49,
        (SELECT id FROM route_plans WHERE vehicle_id='LORRY-01' LIMIT 1),
        NOW() - INTERVAL '3 hours', NOW() - INTERVAL '3 hours', NOW() - INTERVAL '2.5 hours', NOW() - INTERVAL '2 hours', NOW() - INTERVAL '30 minutes'),
    ('routine', 2, 'IN_PROGRESS', 5, 'LORRY-02', 'DRV-002', 284.04,
        (SELECT id FROM route_plans WHERE vehicle_id='LORRY-02' LIMIT 1),
        NOW() - INTERVAL '1.5 hours', NOW() - INTERVAL '1.5 hours', NOW() - INTERVAL '1 hour', NOW() - INTERVAL '45 minutes', NULL),
    ('routine', 3, 'ASSIGNED', 5, 'LORRY-03', 'DRV-003', 103.98,
        (SELECT id FROM route_plans WHERE vehicle_id='LORRY-03' LIMIT 1),
        NOW() - INTERVAL '45 minutes', NOW() - INTERVAL '45 minutes', NOW() - INTERVAL '30 minutes', NULL, NULL),
    ('emergency', 4, 'CREATED', 1, NULL, NULL, 513.30, NULL,
        NOW() - INTERVAL '15 minutes', NULL, NULL, NULL, NULL)
ON CONFLICT DO NOTHING;

-- Routine collection schedules
INSERT INTO routine_schedules (zone_id, waste_category_id, frequency, day_of_week, time_of_day, active)
VALUES
    (1, 1, 'weekly', 'Monday', '08:00', TRUE),
    (1, 5, 'weekly', 'Monday', '10:00', TRUE),
    (2, 2, 'weekly', 'Tuesday', '08:00', TRUE),
    (2, 4, 'weekly', 'Tuesday', '10:00', TRUE),
    (3, 1, 'weekly', 'Wednesday', '08:00', TRUE),
    (3, 3, 'weekly', 'Wednesday', '14:00', TRUE),
    (4, 5, 'weekly', 'Thursday', '08:00', TRUE),
    (1, NULL, 'biweekly', 'Friday', '09:00', TRUE)
ON CONFLICT DO NOTHING;

-- Model performance tracking
INSERT INTO model_performance (model_version, trained_at, training_records, mae_hours, promoted_to_prod, promoted_at)
VALUES
    ('v1.0.0', NOW() - INTERVAL '30 days', 50000, 1.25, TRUE, NOW() - INTERVAL '28 days'),
    ('v1.1.0', NOW() - INTERVAL '14 days', 75000, 0.95, TRUE, NOW() - INTERVAL '7 days'),
    ('v2.0.0-beta', NOW() - INTERVAL '2 days', 120000, 0.72, FALSE, NULL)
ON CONFLICT DO NOTHING;

-- Zone snapshots
INSERT INTO zone_snapshots (zone_id, snapshot_at, avg_fill_level, urgent_bin_count, total_bins, dominant_waste_category, total_estimated_kg, window_minutes)
VALUES
    (1, NOW() - INTERVAL '30 minutes', 72.0, 1, 3, 'food_waste', 268.0, 30),
    (2, NOW() - INTERVAL '30 minutes', 60.5, 0, 3, 'general', 284.0, 30),
    (3, NOW() - INTERVAL '30 minutes', 78.9, 2, 3, 'general', 345.0, 30),
    (4, NOW() - INTERVAL '30 minutes', 71.6, 1, 2, 'general', 513.0, 30),
    (1, NOW() - INTERVAL '1 hour', 70.2, 1, 3, 'food_waste', 265.0, 30)
ON CONFLICT DO NOTHING;