------ SECURITY ANOMALY DETECTION SCHEMA (Phase 3B) ------
-- Security incidents table
CREATE TABLE IF NOT EXISTS anomaly_security_events (
    id BIGSERIAL PRIMARY KEY,
    event_type VARCHAR(100) NOT NULL,
    -- COORDINATED_ATTACK, IMPOSSIBLE_PHYSICS, METADATA_TAMPERING, ENTROPY_ANOMALY, NETWORK_ANOMALY
    severity VARCHAR(20) NOT NULL,
    -- LOW, MEDIUM, HIGH, CRITICAL
    attack_vector TEXT,
    -- description of attack pattern
    affected_bins JSONB DEFAULT '[]'::JSONB,
    -- array of bin IDs
    affected_zones JSONB DEFAULT '[]'::JSONB,
    -- array of zone IDs
    source_ips JSONB DEFAULT '[]'::JSONB,
    -- suspicious IPs
    suspicious_tokens JSONB DEFAULT '[]'::JSONB,
    -- suspicious API tokens
    evidence_json JSONB DEFAULT '{}'::JSONB,
    -- detailed evidence: readings, timestamps, probability scores
    bayesian_probability DECIMAL(5, 2),
    -- 0.0 to 1.0 confidence that this is a real attack
    resolution_status VARCHAR(20) DEFAULT 'open',
    -- open, investigating, mitigated, confirmed, false_alarm
    resolved_at TIMESTAMPTZ,
    resolved_by VARCHAR(100),
    resolution_notes TEXT,
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_security_events_timestamp ON anomaly_security_events(timestamp DESC);
CREATE INDEX idx_security_events_type ON anomaly_security_events(event_type);
CREATE INDEX idx_security_events_severity ON anomaly_security_events(severity);
CREATE INDEX idx_security_events_status ON anomaly_security_events(resolution_status);

-- Link security incidents to anomaly events for correlation
CREATE TABLE IF NOT EXISTS security_anomaly_correlation (
    id BIGSERIAL PRIMARY KEY,
    security_event_id BIGINT NOT NULL REFERENCES anomaly_security_events(id) ON DELETE CASCADE,
    anomaly_event_id BIGINT NOT NULL REFERENCES anomaly_events(id) ON DELETE CASCADE,
    correlation_strength DECIMAL(5, 2),
    -- 0.0 to 1.0
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (security_event_id, anomaly_event_id)
);

CREATE INDEX idx_security_correlation_security_id ON security_anomaly_correlation(security_event_id);
CREATE INDEX idx_security_correlation_anomaly_id ON security_anomaly_correlation(anomaly_event_id);

-- Network activity log for tracking API/network anomalies
CREATE TABLE IF NOT EXISTS network_activity_log (
    id BIGSERIAL PRIMARY KEY,
    api_token_hash VARCHAR(255),
    source_ip VARCHAR(45),
    -- supports IPv6
    bin_id VARCHAR(20),
    reading_count INTEGER,
    request_timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_network_activity_token ON network_activity_log(api_token_hash);
CREATE INDEX idx_network_activity_ip ON network_activity_log(source_ip);
CREATE INDEX idx_network_activity_timestamp ON network_activity_log(request_timestamp DESC);

-- Entropy scores for detecting spoofed data
CREATE TABLE IF NOT EXISTS entropy_scores (
    id BIGSERIAL PRIMARY KEY,
    bin_id VARCHAR(20) NOT NULL REFERENCES bins(id) ON DELETE CASCADE,
    measurement_window_minutes INTEGER DEFAULT 50,
    fill_level_entropy DECIMAL(10, 5),
    -- Shannon entropy of fill_level distribution
    temperature_entropy DECIMAL(10, 5),
    anomalous BOOLEAN DEFAULT FALSE,
    flagged_reason VARCHAR(100),
    -- too_uniform, too_random, etc
    timestamp TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX idx_entropy_scores_bin_id ON entropy_scores(bin_id);
CREATE INDEX idx_entropy_scores_timestamp ON entropy_scores(timestamp DESC);
CREATE INDEX idx_entropy_scores_anomalous ON entropy_scores(anomalous);
