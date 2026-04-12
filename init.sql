-- Create the waste_stream table
CREATE TABLE IF NOT EXISTS waste_stream (
    id SERIAL PRIMARY KEY,
    bin_id VARCHAR(255) NOT NULL,
    fill INTEGER NOT NULL,
    location VARCHAR(255),
    priority VARCHAR(50),
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create an index on bin_id for faster queries
CREATE INDEX IF NOT EXISTS idx_waste_stream_bin_id ON waste_stream(bin_id);

-- Create an index on timestamp for time-series queries
CREATE INDEX IF NOT EXISTS idx_waste_stream_timestamp ON waste_stream(timestamp DESC);

-- Prediction tables split by horizon for easier querying and independent retention.
CREATE TABLE IF NOT EXISTS waste_predictions_5min (
    id SERIAL PRIMARY KEY,
    bin_id VARCHAR(255) NOT NULL,
    predicted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    predicted_for TIMESTAMP WITH TIME ZONE NOT NULL,
    horizon_label VARCHAR(50) NOT NULL,
    predicted_fill FLOAT NOT NULL,
    confidence FLOAT,
    model_version VARCHAR(100),
    creation_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_predictions_5min_bin_time ON waste_predictions_5min(bin_id, predicted_at DESC);

CREATE TABLE IF NOT EXISTS waste_predictions_4hour (
    id SERIAL PRIMARY KEY,
    bin_id VARCHAR(255) NOT NULL,
    predicted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    predicted_for TIMESTAMP WITH TIME ZONE NOT NULL,
    horizon_label VARCHAR(50) NOT NULL,
    predicted_fill FLOAT NOT NULL,
    confidence FLOAT,
    model_version VARCHAR(100),
    creation_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_predictions_4hour_bin_time ON waste_predictions_4hour(bin_id, predicted_at DESC);

CREATE TABLE IF NOT EXISTS waste_predictions_1day (
    id SERIAL PRIMARY KEY,
    bin_id VARCHAR(255) NOT NULL,
    predicted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    predicted_for TIMESTAMP WITH TIME ZONE NOT NULL,
    horizon_label VARCHAR(50) NOT NULL,
    predicted_fill FLOAT NOT NULL,
    confidence FLOAT,
    model_version VARCHAR(100),
    creation_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_predictions_1day_bin_time ON waste_predictions_1day(bin_id, predicted_at DESC);

CREATE TABLE IF NOT EXISTS waste_predictions_7day (
    id SERIAL PRIMARY KEY,
    bin_id VARCHAR(255) NOT NULL,
    predicted_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    predicted_for TIMESTAMP WITH TIME ZONE NOT NULL,
    horizon_label VARCHAR(50) NOT NULL,
    predicted_fill FLOAT NOT NULL,
    confidence FLOAT,
    model_version VARCHAR(100),
    creation_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_predictions_7day_bin_time ON waste_predictions_7day(bin_id, predicted_at DESC);

-- Table for tracking model training metadata
CREATE TABLE IF NOT EXISTS model_metrics (
    id SERIAL PRIMARY KEY,
    bin_id VARCHAR(255) NOT NULL,
    trained_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    mae FLOAT, -- Mean Absolute Error
    rmse FLOAT, -- Root Mean Squared Error
    data_points_used INTEGER,
    model_type VARCHAR(50), -- "arima" or "prophet"
    next_retraining TIMESTAMP WITH TIME ZONE,
    creation_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_model_metrics_bin ON model_metrics(bin_id, trained_at DESC);
