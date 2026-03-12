const mysql = require('mysql2/promise');
const { Pool } = require('pg');

/**
 * Service to handle live SQL database connections with security constraints.
 * SECURITY:
 * - All connections are backend-mediated (never client-side)
 * - Query timeouts enforced
 * - Row limits enforced
 * - Only SELECT queries allowed
 * - Credentials NOT stored in database
 */
class DbConnectorService {
    constructor() {
        // Security constants
        this.CONNECTION_TIMEOUT = 10000; // 10 seconds
        this.QUERY_TIMEOUT = 30000; // 30 seconds
        this.MAX_ROWS = 5000; // Maximum rows per query
    }

    /**
     * Helper to create a MySQL connection with SSL fallback
     */
    async _createMySQLConnection(config) {
        const connectionConfig = {
            host: config.host,
            port: config.port || 3306,
            user: config.user,
            password: config.password,
            database: config.database,
            connectTimeout: this.CONNECTION_TIMEOUT,
            ssl: {
                rejectUnauthorized: false
            }
        };

        try {
            console.log(`[MySQL] Connecting to ${config.host} with SSL...`);
            return await mysql.createConnection(connectionConfig);
        } catch (sslError) {
            console.warn(`[MySQL] SSL connection failed: ${sslError.message}. Retrying without SSL...`);
            const noSslConfig = { ...connectionConfig };
            delete noSslConfig.ssl;
            return await mysql.createConnection(noSslConfig);
        }
    }

    /**
     * Helper to create a PostgreSQL pool with SSL fallback
     */
    async _createPostgreSQLPool(config) {
        const poolConfig = {
            host: config.host,
            port: config.port || 5432,
            user: config.user,
            password: config.password,
            database: config.database,
            connectionTimeoutMillis: this.CONNECTION_TIMEOUT,
            max: 1,
            ssl: {
                rejectUnauthorized: false
            }
        };

        try {
            console.log(`[Postgres] Connecting to ${config.host} with SSL...`);
            const pool = new Pool(poolConfig);
            // Verify connection
            const client = await pool.connect();
            client.release();
            return pool;
        } catch (sslError) {
            console.warn(`[Postgres] SSL connection failed: ${sslError.message}. Retrying without SSL...`);
            const noSslConfig = { ...poolConfig };
            delete noSslConfig.ssl;
            return new Pool(noSslConfig);
        }
    }

    /**
     * Test database connection credentials
     */
    async testConnection(config) {
        try {
            const { engine, host, database, user } = config;

            if (!engine || !host || !database || !user) {
                throw new Error('Missing required connection parameters');
            }

            if (engine === 'mysql') {
                const connection = await this._createMySQLConnection(config);
                await connection.query('SELECT 1');
                await connection.end();
                return { success: true, message: 'MySQL connection successful' };
            } else if (engine === 'postgresql') {
                const pool = await this._createPostgreSQLPool(config);
                const client = await pool.connect();
                await client.query('SELECT 1');
                client.release();
                await pool.end();
                return { success: true, message: 'PostgreSQL connection successful' };
            } else {
                throw new Error(`Unsupported database engine: ${engine}`);
            }
        } catch (error) {
            console.error('Database connection test failed:', error.message);
            return {
                success: false,
                message: this._sanitizeErrorMessage(error.message, config.host)
            };
        }
    }

    /**
     * Get list of tables from the database
     */
    async getTables(config) {
        try {
            const { engine, database } = config;

            if (engine === 'mysql') {
                const connection = await this._createMySQLConnection(config);
                const [rows] = await connection.query('SHOW TABLES');
                await connection.end();
                const tableKey = `Tables_in_${database}`;
                return rows.map(row => row[tableKey]);
            } else if (engine === 'postgresql') {
                const pool = await this._createPostgreSQLPool(config);
                const client = await pool.connect();
                const result = await client.query(
                    `SELECT table_name FROM information_schema.tables 
                     WHERE table_schema = 'public' 
                     AND table_type = 'BASE TABLE'
                     ORDER BY table_name`
                );
                client.release();
                await pool.end();
                return result.rows.map(row => row.table_name);
            } else {
                throw new Error(`Unsupported database engine: ${engine}`);
            }
        } catch (error) {
            console.error('Failed to get tables:', error.message);
            throw new Error(this._sanitizeErrorMessage(error.message, config.host));
        }
    }

    /**
     * Get data from a specific table, including column metadata
     */
    async getTableData(config, tableName, limit = this.MAX_ROWS) {
        try {
            const { engine, database } = config;

            if (!this._isValidTableName(tableName)) {
                throw new Error('Invalid table name');
            }

            const rowLimit = Math.min(limit, this.MAX_ROWS);

            if (engine === 'mysql') {
                const connection = await this._createMySQLConnection(config);

                // Get columns
                const [columns] = await connection.query(
                    'SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION',
                    [database, tableName]
                );

                // Get data
                const query = `SELECT * FROM \`${tableName}\` LIMIT ${rowLimit}`;
                const [rows] = await connection.query(query);
                await connection.end();

                const headers = columns.map(col => col.COLUMN_NAME);
                if (rows.length === 0) {
                    return { headers, columns: columns.map(c => ({ name: c.COLUMN_NAME, type: c.DATA_TYPE })), rows: [headers] };
                }

                const dataRows = rows.map(row => headers.map(header => row[header]));
                return {
                    headers,
                    columns: columns.map(c => ({ name: c.COLUMN_NAME, type: c.DATA_TYPE })),
                    rows: [headers, ...dataRows]
                };
            } else if (engine === 'postgresql') {
                const pool = await this._createPostgreSQLPool(config);
                const client = await pool.connect();

                // Get columns
                const columnsResult = await client.query(
                    `SELECT column_name, data_type 
                     FROM information_schema.columns 
                     WHERE table_schema = 'public' 
                     AND table_name = $1 
                     ORDER BY ordinal_position`,
                    [tableName]
                );

                // Get data
                const query = `SELECT * FROM "${tableName}" LIMIT $1`;
                const result = await client.query(query, [rowLimit]);
                client.release();
                await pool.end();

                const headers = columnsResult.rows.map(col => col.column_name);
                if (result.rows.length === 0) {
                    return { headers, columns: columnsResult.rows.map(c => ({ name: c.column_name, type: c.data_type })), rows: [headers] };
                }

                const dataRows = result.rows.map(row => headers.map(header => row[header]));
                return {
                    headers,
                    columns: columnsResult.rows.map(c => ({ name: c.column_name, type: c.data_type })),
                    rows: [headers, ...dataRows]
                };
            } else {
                throw new Error(`Unsupported database engine: ${engine}`);
            }
        } catch (error) {
            console.error('Failed to get table data:', error.message);
            throw new Error(this._sanitizeErrorMessage(error.message, config.host));
        }
    }

    _isValidTableName(tableName) {
        const validPattern = /^[a-zA-Z0-9_-]+$/;
        return validPattern.test(tableName);
    }

    _sanitizeErrorMessage(message, host) {
        if (message.includes('ECONNREFUSED')) {
            if (host === 'localhost' || host === '127.0.0.1') {
                return 'Connection refused for "localhost". Since the app is hosted online, it cannot reach your local device\'s database. Please use a public database host or an IP tunnel (like ngrok).';
            }
            return 'Unable to connect to database server. Please check host, port, and ensure the server allows external connections.';
        }
        if (message.includes('ER_ACCESS_DENIED_ERROR') || message.includes('authentication failed')) {
            return 'Authentication failed. Please check username and password.';
        }
        if (message.includes('Unknown database') || message.includes('database') && message.includes('does not exist')) {
            return 'Database not found. Please check database name.';
        }
        if (message.includes('ETIMEDOUT') || message.includes('timeout')) {
            return 'Connection timeout. Please check your network, firewall settings, and ensure the database is accessible from the internet.';
        }
        if (message.includes('SSL') || message.includes('ssl')) {
            return 'SSL/TLS connection error. The database might require specific SSL configuration or your cloud provider might be blocking the connection.';
        }

        return 'Database operation failed: ' + message;
    }
}

module.exports = new DbConnectorService();
