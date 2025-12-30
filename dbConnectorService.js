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
     * Test database connection credentials
     * @param {Object} config - Database configuration
     * @param {string} config.engine - 'mysql' or 'postgresql'
     * @param {string} config.host - Database host
     * @param {number} config.port - Database port
     * @param {string} config.database - Database name
     * @param {string} config.user - Database username
     * @param {string} config.password - Database password
     * @returns {Promise<{success: boolean, message: string}>}
     */
    async testConnection(config) {
        try {
            const { engine, host, port, database, user, password } = config;

            if (!engine || !host || !database || !user) {
                throw new Error('Missing required connection parameters');
            }

            if (engine === 'mysql') {
                const connection = await mysql.createConnection({
                    host,
                    port: port || 3306,
                    user,
                    password,
                    database,
                    connectTimeout: this.CONNECTION_TIMEOUT
                });

                // Test the connection with a simple query
                await connection.query('SELECT 1');
                await connection.end();

                return { success: true, message: 'MySQL connection successful' };
            } else if (engine === 'postgresql') {
                const pool = new Pool({
                    host,
                    port: port || 5432,
                    user,
                    password,
                    database,
                    connectionTimeoutMillis: this.CONNECTION_TIMEOUT,
                    max: 1 // Only one connection for testing
                });

                // Test the connection
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
                message: this._sanitizeErrorMessage(error.message)
            };
        }
    }

    /**
     * Get list of tables from the database
     * @param {Object} config - Database configuration
     * @returns {Promise<string[]>} Array of table names
     */
    async getTables(config) {
        try {
            const { engine, host, port, database, user, password } = config;

            if (engine === 'mysql') {
                const connection = await mysql.createConnection({
                    host,
                    port: port || 3306,
                    user,
                    password,
                    database,
                    connectTimeout: this.CONNECTION_TIMEOUT
                });

                const [rows] = await connection.query(
                    'SHOW TABLES',
                    [],
                    { timeout: this.QUERY_TIMEOUT }
                );

                await connection.end();

                // MySQL returns results as array of objects with key "Tables_in_<dbname>"
                const tableKey = `Tables_in_${database}`;
                return rows.map(row => row[tableKey]);
            } else if (engine === 'postgresql') {
                const pool = new Pool({
                    host,
                    port: port || 5432,
                    user,
                    password,
                    database,
                    connectionTimeoutMillis: this.CONNECTION_TIMEOUT,
                    query_timeout: this.QUERY_TIMEOUT,
                    max: 1
                });

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
            throw new Error(this._sanitizeErrorMessage(error.message));
        }
    }

    /**
     * Get data from a specific table
     * @param {Object} config - Database configuration
     * @param {string} tableName - Name of the table to query
     * @param {number} limit - Maximum number of rows (default: MAX_ROWS)
     * @returns {Promise<{headers: string[], rows: any[][]}>} Table data in array-of-arrays format
     */
    async getTableData(config, tableName, limit = this.MAX_ROWS) {
        try {
            const { engine, host, port, database, user, password } = config;

            // Security: Validate table name to prevent SQL injection
            if (!this._isValidTableName(tableName)) {
                throw new Error('Invalid table name');
            }

            // Enforce row limit
            const rowLimit = Math.min(limit, this.MAX_ROWS);

            if (engine === 'mysql') {
                const connection = await mysql.createConnection({
                    host,
                    port: port || 3306,
                    user,
                    password,
                    database,
                    connectTimeout: this.CONNECTION_TIMEOUT
                });

                // Use parameterized query for table name safety
                // Note: mysql2 doesn't support parameter binding for table names
                // so we validate the table name separately
                const query = `SELECT * FROM \`${tableName}\` LIMIT ${rowLimit}`;
                const [rows] = await connection.query(query, [], { timeout: this.QUERY_TIMEOUT });

                await connection.end();

                if (rows.length === 0) {
                    return { headers: [], rows: [] };
                }

                // Extract headers from first row
                const headers = Object.keys(rows[0]);

                // Convert rows to array-of-arrays format (matching Excel/Sheets)
                const dataRows = rows.map(row => headers.map(header => row[header]));

                return {
                    headers,
                    rows: [headers, ...dataRows] // Include headers as first row
                };
            } else if (engine === 'postgresql') {
                const pool = new Pool({
                    host,
                    port: port || 5432,
                    user,
                    password,
                    database,
                    connectionTimeoutMillis: this.CONNECTION_TIMEOUT,
                    query_timeout: this.QUERY_TIMEOUT,
                    max: 1
                });

                const client = await pool.connect();

                // PostgreSQL: Use double quotes for identifiers
                const query = `SELECT * FROM "${tableName}" LIMIT $1`;
                const result = await client.query(query, [rowLimit]);

                client.release();
                await pool.end();

                if (result.rows.length === 0) {
                    return { headers: [], rows: [] };
                }

                // Extract headers from result fields
                const headers = result.fields.map(field => field.name);

                // Convert rows to array-of-arrays format
                const dataRows = result.rows.map(row => headers.map(header => row[header]));

                return {
                    headers,
                    rows: [headers, ...dataRows] // Include headers as first row
                };
            } else {
                throw new Error(`Unsupported database engine: ${engine}`);
            }
        } catch (error) {
            console.error('Failed to get table data:', error.message);
            throw new Error(this._sanitizeErrorMessage(error.message));
        }
    }

    /**
     * Validate table name to prevent SQL injection
     * @private
     */
    _isValidTableName(tableName) {
        // Allow only alphanumeric characters, underscores, and hyphens
        // No spaces, semicolons, or other SQL metacharacters
        const validPattern = /^[a-zA-Z0-9_-]+$/;
        return validPattern.test(tableName);
    }

    /**
     * Sanitize error messages to avoid leaking sensitive information
     * @private
     */
    _sanitizeErrorMessage(message) {
        // Remove any potential sensitive information from error messages
        // while keeping the error useful for debugging

        // Common error patterns to make user-friendly
        if (message.includes('ECONNREFUSED')) {
            return 'Unable to connect to database server. Please check host and port.';
        }
        if (message.includes('ER_ACCESS_DENIED_ERROR') || message.includes('authentication failed')) {
            return 'Authentication failed. Please check username and password.';
        }
        if (message.includes('Unknown database')) {
            return 'Database not found. Please check database name.';
        }
        if (message.includes('ETIMEDOUT') || message.includes('timeout')) {
            return 'Connection timeout. Please check your network and database availability.';
        }

        // If no specific pattern matches, return a generic message
        // but log the full error server-side
        return 'Database operation failed. Please check your connection settings.';
    }
}

module.exports = new DbConnectorService();
