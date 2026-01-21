-- SharePoint Per-User OAuth Support
-- Migration to add support for storing per-user SharePoint OAuth tokens

-- ========================================
-- SharePoint Connections Table
-- Stores OAuth tokens for each user's SharePoint connection
-- ========================================
CREATE TABLE IF NOT EXISTS sharepoint_connections (
    id BIGSERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    tenant_id VARCHAR(255),
    access_token TEXT NOT NULL,  -- encrypted with AES-256-CBC
    refresh_token TEXT NOT NULL,  -- encrypted with AES-256-CBC
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    connected_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(user_id)  -- One connection per user
);

-- ========================================
-- Indexes
-- ========================================
CREATE INDEX IF NOT EXISTS idx_sharepoint_connections_user_id ON sharepoint_connections(user_id);
CREATE INDEX IF NOT EXISTS idx_sharepoint_connections_expires_at ON sharepoint_connections(expires_at);

-- ========================================
-- Ensure source_info column exists in uploaded_files
-- ========================================
DO $$ 
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM information_schema.columns 
        WHERE table_name = 'uploaded_files' 
        AND column_name = 'source_info'
    ) THEN
        ALTER TABLE uploaded_files ADD COLUMN source_info JSONB DEFAULT '{}';
    END IF;
END $$;

-- ========================================
-- Row Level Security (RLS)
-- ========================================
ALTER TABLE sharepoint_connections ENABLE ROW LEVEL SECURITY;

-- Users can only view their own SharePoint connection
DROP POLICY IF EXISTS "Users can view own sharepoint connection" ON sharepoint_connections;
CREATE POLICY "Users can view own sharepoint connection" ON sharepoint_connections 
    FOR SELECT USING (user_id::text = auth.uid()::text);

-- Users can create their own SharePoint connection
DROP POLICY IF EXISTS "Users can create sharepoint connection" ON sharepoint_connections;
CREATE POLICY "Users can create sharepoint connection" ON sharepoint_connections 
    FOR INSERT WITH CHECK (user_id::text = auth.uid()::text);

-- Users can update their own SharePoint connection
DROP POLICY IF EXISTS "Users can update own sharepoint connection" ON sharepoint_connections;
CREATE POLICY "Users can update own sharepoint connection" ON sharepoint_connections 
    FOR UPDATE USING (user_id::text = auth.uid()::text);

-- Users can delete their own SharePoint connection
DROP POLICY IF EXISTS "Users can delete own sharepoint connection" ON sharepoint_connections;
CREATE POLICY "Users can delete own sharepoint connection" ON sharepoint_connections 
    FOR DELETE USING (user_id::text = auth.uid()::text);

-- ========================================
-- Migration Complete
-- ========================================
-- Run this migration in your Supabase SQL Editor or via CLI
