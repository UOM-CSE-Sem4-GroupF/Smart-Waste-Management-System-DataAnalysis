------ ANOMALY DETECTION SCHEMA (Phase 1) ------
-- Main anomaly events table
CREATE TABLE IF NOT EXISTS anomaly_events (
    id BIGSERIAL PRIMARY KEY,
    bin_id VARCHAR(20),
    zone_id INTEGER,
    vehicle_id VARCHAR(20),
    anomaly_type VARCHAR(100) NOT NULL,
    -- hardware_failure, data_quality, operational, security, event_based
    severity VARCHAR(20) NOT NULL,
    -- LOW, MEDIUM, HIGH, CRITICAL
    anomaly_flags JSONB DEFAULT '[]'::JSONB,
    -- array of specific flags detected
    anomaly_score DECIMAL(5, 2) DEFAULT NULL,
    -- 0.0 to 1.0 for ML-based detection
    metadata_json JSONB DEFAULT '{}'::JSONB,
    -- context-specific data
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    FOREIGN KEY (bin_id) REFERENCES bins(id) ON DELETE SET NULL,
    FOREIGN KEY (zone_id) REFERENCES city_zones(id) ON DELETE SET NULL,
    FOREIGN KEY (vehicle_id) REFERENCES vehicles(id) ON DELETE SET NULL
);

CREATE INDEX idx_anomaly_events_timestamp ON anomaly_events(timestamp DESC);
CREATE INDEX idx_anomaly_events_bin_id ON anomaly_events(bin_id);
CREATE INDEX idx_anomaly_events_zone_id ON anomaly_events(zone_id);
CREATE INDEX idx_anomaly_events_severity ON anomaly_events(severity);
CREATE INDEX idx_anomaly_events_type ON anomaly_events(anomaly_type);

-- Baseline statistics for each bin/metric
CREATE TABLE IF NOT EXISTS anomaly_baselines (
    id BIGSERIAL PRIMARY KEY,
    bin_id VARCHAR(20) NOT NULL REFERENCES bins(id) ON DELETE CASCADE,
    metric_name VARCHAR(100) NOT NULL,
    -- fill_level, temperature, fill_rate, etc
    baseline_value DECIMAL(10, 3) NOT NULL,
    std_dev DECIMAL(10, 3),
    lower_bound DECIMAL(10, 3),
    upper_bound DECIMAL(10, 3),
    sample_size INTEGER,
    last_updated TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (bin_id, metric_name)
);

CREATE INDEX idx_anomaly_baselines_bin_id ON anomaly_baselines(bin_id);

-- Alert rules for triggering actions
CREATE TABLE IF NOT EXISTS anomaly_rules (
    id BIGSERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    condition_type VARCHAR(50) NOT NULL,
    -- threshold_based, pattern_based, ml_based
    condition_json JSONB NOT NULL,
    -- rule logic: {"anomaly_type": "...", "min_severity": "HIGH", "threshold": 3}
    severity VARCHAR(20) NOT NULL,
    action VARCHAR(50) NOT NULL,
    -- email, slack, sms, webhook, escalate
    action_target TEXT,
    -- email address, slack channel, webhook URL, etc
    enabled BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_anomaly_rules_enabled ON anomaly_rules(enabled);

-- Alert tracking and resolution
CREATE TABLE IF NOT EXISTS anomaly_alerts (
    id BIGSERIAL PRIMARY KEY,
    anomaly_id BIGINT NOT NULL REFERENCES anomaly_events(id) ON DELETE CASCADE,
    rule_id BIGINT REFERENCES anomaly_rules(id) ON DELETE SET NULL,
    alert_status VARCHAR(20) NOT NULL DEFAULT 'pending',
    -- pending, acknowledged, resolved, escalated
    action_taken TEXT,
    taken_by VARCHAR(100),
    resolution_notes TEXT,
    resolved_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_anomaly_alerts_anomaly_id ON anomaly_alerts(anomaly_id);
CREATE INDEX idx_anomaly_alerts_status ON anomaly_alerts(alert_status);
CREATE INDEX idx_anomaly_alerts_created ON anomaly_alerts(created_at DESC);
