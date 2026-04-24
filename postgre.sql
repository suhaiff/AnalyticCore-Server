-- Optional: use pgcrypto if you want gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- USERS TABLE
CREATE TABLE users (
  id serial PRIMARY KEY,
  name varchar(255) NOT NULL,
  email varchar(255) NOT NULL UNIQUE,
  password varchar(255) NOT NULL,
  role varchar(10) NOT NULL DEFAULT 'USER',
  created_at timestamptz DEFAULT now()
);

-- Default admin user (update password hashing in production)
INSERT INTO users (name, email, password, role)
VALUES ('Admin', 'admin@gmail.com', 'admin123', 'ADMIN');

-- DASHBOARDS TABLE

CREATE TABLE dashboards (
  id serial PRIMARY KEY,
  user_id integer NOT NULL,
  name varchar(255) NOT NULL,
  data_model jsonb NOT NULL,
  chart_configs jsonb NOT NULL,
  created_at timestamptz DEFAULT now(),
  CONSTRAINT dashboards_user_fk FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
);
CREATE INDEX idx_dashboards_user_id ON dashboards(user_id);

-- UPLOADED FILES TABLE (Metadata)

CREATE TABLE uploaded_files (
  id serial PRIMARY KEY,
  user_id integer NOT NULL,
  original_name varchar(255) NOT NULL,
  mime_type varchar(100),
  file_size bigint,
  sheet_count integer DEFAULT 0,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now(),
  CONSTRAINT uploaded_files_user_fk FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
);
CREATE INDEX idx_uploaded_files_user_created ON uploaded_files(user_id, created_at DESC);

-- Trigger to auto-update updated_at on row change
CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$Q
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$;


CREATE TRIGGER trg_touch_uploaded_files_updated_at
BEFORE UPDATE ON uploaded_files
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- EXCEL SHEETS TABLE

CREATE TABLE excel_sheets (
  id serial PRIMARY KEY,
  file_id integer NOT NULL,
  sheet_name varchar(255) NOT NULL,
  sheet_index integer NOT NULL,
  row_count integer DEFAULT 0,
  column_count integer DEFAULT 0,
  created_at timestamptz DEFAULT now(),
  CONSTRAINT excel_sheets_file_fk FOREIGN KEY (file_id) REFERENCES uploaded_files (id) ON DELETE CASCADE
);
CREATE INDEX idx_excel_sheets_file_id ON excel_sheets(file_id);
CREATE INDEX idx_excel_sheets_file_sheet ON excel_sheets(file_id, sheet_index);

-- EXCEL DATA TABLE

CREATE TABLE excel_data (
  id bigserial PRIMARY KEY,
  sheet_id integer NOT NULL,
  row_index integer NOT NULL,
  row_data jsonb NOT NULL,
  created_at timestamptz DEFAULT now(),
  CONSTRAINT excel_data_sheet_fk FOREIGN KEY (sheet_id) REFERENCES excel_sheets (id) ON DELETE CASCADE
);
CREATE INDEX idx_excel_data_sheet_id ON excel_data(sheet_id);
CREATE INDEX idx_excel_data_sheet_row ON excel_data(sheet_id, row_index);

-- FILE UPLOAD LOG TABLE

CREATE TABLE file_upload_log (
  id serial PRIMARY KEY,
  file_id integer NOT NULL,
  upload_date date NOT NULL,
  upload_time time NOT NULL,
  file_path varchar(500),
  status varchar(10) NOT NULL DEFAULT 'SUCCESS',
  error_message text,
  CONSTRAINT file_upload_log_file_fk FOREIGN KEY (file_id) REFERENCES uploaded_files (id) ON DELETE CASCADE
);
CREATE INDEX idx_file_upload_log_file_id ON file_upload_log(file_id);
CREATE INDEX idx_file_upload_log_upload_date ON file_upload_log(upload_date);

-- DATA CONFIGURATION LOG TABLE

CREATE TABLE data_configuration_log (
  id serial PRIMARY KEY,
  file_name varchar(255) NOT NULL,
  config_date date NOT NULL,
  config_time time NOT NULL,
  columns jsonb NOT NULL,
  join_configs jsonb,
  created_at timestamptz DEFAULT now()
);
CREATE INDEX idx_data_configuration_log_config_date ON data_configuration_log(config_date);

-- INDICES FOR PERFORMANCE (already added above, repeating safe indexes)
CREATE INDEX IF NOT EXISTS idx_files_user_created ON uploaded_files(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_data_sheet_row ON excel_data(sheet_id, row_index);

-- VIEWS FOR EASY QUERYING
CREATE OR REPLACE VIEW v_file_details AS
SELECT
  uf.id as file_id,
  uf.original_name,
  uf.mime_type,
  uf.file_size,
  uf.sheet_count,
  uf.created_at,
  u.id as user_id,
  u.name as user_name,
  u.email as user_email,
  (SELECT COUNT(*) FROM excel_sheets WHERE file_id = uf.id) as actual_sheet_count,
  (SELECT COALESCE(SUM(row_count),0) FROM excel_sheets WHERE file_id = uf.id) as total_rows
FROM uploaded_files uf
JOIN users u ON uf.user_id = u.id;

CREATE OR REPLACE VIEW v_sheet_details AS
SELECT
  es.id as sheet_id,
  es.sheet_name,
  es.sheet_index,
  es.row_count,
  es.column_count,
  uf.id as file_id,
  uf.original_name as file_name,
  uf.user_id,
  u.name as user_name
FROM excel_sheets es
JOIN uploaded_files uf ON es.file_id = uf.id
JOIN users u ON uf.user_id = u.id;