------ EVENT-BASED ANOMALY DETECTION SCHEMA (Phase 3C) ------
-- Special events logging table
CREATE TABLE IF NOT EXISTS special_events (
    id BIGSERIAL PRIMARY KEY,
    event_name VARCHAR(255) NOT NULL,
    zone_ids JSONB NOT NULL,
    -- array of zone IDs affected
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    event_type VARCHAR(100) NOT NULL,
    -- FESTIVAL, CONCERT, PROTEST, ROAD_CLOSURE, NATURAL_DISASTER, STREET_CLEANING,
    -- SPORTS_EVENT, MARKET_DAY, CONSTRUCTION, OTHER
    description TEXT,
    created_by VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_special_events_start ON special_events(start_time);
CREATE INDEX idx_special_events_end ON special_events(end_time);
CREATE INDEX idx_special_events_type ON special_events(event_type);

-- Event impact analysis table
CREATE TABLE IF NOT EXISTS event_impact_analysis (
    id BIGSERIAL PRIMARY KEY,
    event_id BIGINT NOT NULL REFERENCES special_events(id) ON DELETE CASCADE,
    zone_id INTEGER NOT NULL REFERENCES city_zones(id) ON DELETE CASCADE,
    analysis_date DATE NOT NULL,
    
    -- Anomaly metrics during event
    anomalies_detected_count INTEGER DEFAULT 0,
    anomalies_high_severity_count INTEGER DEFAULT 0,
    anomalies_critical_count INTEGER DEFAULT 0,
    
    -- Waste metrics
    baseline_waste_kg DECIMAL(10, 2),
    -- expected waste before event
    actual_waste_kg DECIMAL(10, 2),
    -- actual waste during event
    waste_increase_pct DECIMAL(6, 2),
    -- percentage increase vs baseline
    
    -- Collection metrics
    scheduled_collections_count INTEGER DEFAULT 0,
    executed_collections_count INTEGER DEFAULT 0,
    failed_collections_count INTEGER DEFAULT 0,
    collection_execution_rate DECIMAL(5, 2),
    -- percentage of scheduled collections executed
    
    -- Fill rate anomalies
    bins_with_chaotic_fill INTEGER DEFAULT 0,
    rapid_surge_incidents INTEGER DEFAULT 0,
    -- count of times 5+ bins surged >20% in 30 min
    
    -- Prediction accuracy during event
    ml_prediction_accuracy DECIMAL(5, 2),
    -- actual vs predicted fill times
    
    analysis_notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (event_id, zone_id)
);

CREATE INDEX idx_event_impact_event_id ON event_impact_analysis(event_id);
CREATE INDEX idx_event_impact_zone_id ON event_impact_analysis(zone_id);
CREATE INDEX idx_event_impact_analysis_date ON event_impact_analysis(analysis_date);

-- Auto-detected event-driven anomalies
CREATE TABLE IF NOT EXISTS auto_detected_events (
    id BIGSERIAL PRIMARY KEY,
    zone_id INTEGER NOT NULL REFERENCES city_zones(id) ON DELETE CASCADE,
    detection_type VARCHAR(100) NOT NULL,
    -- CHAOTIC_DEMAND, RAPID_SURGE, FORECAST_DEVIATION
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    affected_bins_count INTEGER,
    waste_volume_actual_kg DECIMAL(10, 2),
    waste_volume_forecasted_kg DECIMAL(10, 2),
    deviation_std_count DECIMAL(5, 2),
    -- how many standard deviations above forecast
    confidence_score DECIMAL(5, 2),
    -- 0.0 to 1.0
    estimated_event_type VARCHAR(100),
    -- FESTIVAL (0.4), ACCIDENT (0.3), CLEANING (0.2), OTHER (0.1)
    estimated_event_probability JSONB DEFAULT '{}'::JSONB,
    -- {"FESTIVAL": 0.4, "ACCIDENT": 0.3, ...}
    processed BOOLEAN DEFAULT FALSE,
    -- has ops team reviewed this event?
    linked_special_event_id BIGINT REFERENCES special_events(id) ON DELETE SET NULL,
    -- if ops team confirms this is a logged event
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_auto_detected_zone_id ON auto_detected_events(zone_id);
CREATE INDEX idx_auto_detected_time ON auto_detected_events(start_time DESC);
CREATE INDEX idx_auto_detected_processed ON auto_detected_events(processed);

-- Link auto-detected events to anomalies
CREATE TABLE IF NOT EXISTS event_anomaly_correlation (
    id BIGSERIAL PRIMARY KEY,
    event_id BIGINT NOT NULL REFERENCES auto_detected_events(id) ON DELETE CASCADE,
    anomaly_id BIGINT NOT NULL REFERENCES anomaly_events(id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (event_id, anomaly_id)
);

CREATE INDEX idx_event_correlation_event_id ON event_anomaly_correlation(event_id);
CREATE INDEX idx_event_correlation_anomaly_id ON event_anomaly_correlation(anomaly_id);
