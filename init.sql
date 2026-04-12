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
