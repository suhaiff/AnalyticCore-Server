CREATE TABLE IF NOT EXISTS api_error_logs (
    id BIGSERIAL PRIMARY KEY,
    error_type VARCHAR(50) NOT NULL,
    error_message TEXT NOT NULL,
    source VARCHAR(100) NOT NULL,
    key_index INT,
    user_id BIGINT,
    user_email VARCHAR(255),
    resolved BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS idx_api_error_logs_created_at ON api_error_logs(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_api_error_logs_resolved ON api_error_logs(resolved);
