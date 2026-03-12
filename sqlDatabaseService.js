const mysql = require('mysql2/promise');

class SqlDatabaseService {
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
            connectTimeout: 10000,
            ssl: {
                rejectUnauthorized: false
            }
        };

        try {
            console.log(`[MySQL Service] Connecting to ${config.host} with SSL...`);
            return await mysql.createConnection(connectionConfig);
        } catch (sslError) {
            console.warn(`[MySQL Service] SSL connection failed: ${sslError.message}. Retrying without SSL...`);
            const noSslConfig = { ...connectionConfig };
            delete noSslConfig.ssl;
            return await mysql.createConnection(noSslConfig);
        }
    }

    /**
     * Helper for PostgreSQL connection
     */
    async _connectPostgreSQL(config) {
        const { Client } = require('pg');
        const clientConfig = {
            host: config.host,
            port: config.port || 5432,
            user: config.user,
            password: config.password,
            database: config.database,
            connectionTimeoutMillis: 10000,
            ssl: {
                rejectUnauthorized: false
            }
        };

        try {
            console.log(`[Postgres Service] Connecting to ${config.host} with SSL...`);
            const client = new Client(clientConfig);
            await client.connect();
            return client;
        } catch (sslError) {
            console.warn(`[Postgres Service] SSL connection failed: ${sslError.message}. Retrying without SSL...`);
            const noSslConfig = { ...clientConfig };
            delete noSslConfig.ssl;
            const client = new Client(noSslConfig);
            await client.connect();
            return client;
        }
    }

    /**
     * Test database connection
     */
    async testConnection(config) {
        const { type } = config;

        if (type === 'mysql' || type === 'mariadb') {
            let connection;
            try {
                connection = await this._createMySQLConnection(config);
                await connection.ping();
                return {
                    success: true,
                    message: 'Connection successful',
                    serverInfo: connection.connection.config
                };
            } catch (error) {
                console.error('MySQL connection test failed:', error.message);
                throw new Error(`MySQL connection failed: ${error.message}`);
            } finally {
                if (connection) await connection.end();
            }
        } else if (type === 'postgresql') {
            try {
                const client = await this._connectPostgreSQL(config);
                const result = await client.query('SELECT version()');
                await client.end();
                return {
                    success: true,
                    message: 'Connection successful',
                    serverInfo: result.rows[0]
                };
            } catch (error) {
                console.error('PostgreSQL connection test failed:', error.message);
                throw new Error(`PostgreSQL connection failed: ${error.message}`);
            }
        } else {
            throw new Error(`Unsupported database type: ${type}`);
        }
    }

    /**
     * Get list of tables from database
     */
    async getTables(config) {
        const { type, database } = config;

        if (type === 'mysql' || type === 'mariadb') {
            let connection;
            try {
                connection = await this._createMySQLConnection(config);
                const [rows] = await connection.query('SHOW TABLES');
                const tableKey = `Tables_in_${database}`;
                return rows.map(row => row[tableKey]);
            } catch (error) {
                console.error('Failed to get MySQL tables:', error.message);
                throw new Error(`Failed to get tables: ${error.message}`);
            } finally {
                if (connection) await connection.end();
            }
        } else if (type === 'postgresql') {
            try {
                const client = await this._connectPostgreSQL(config);
                const result = await client.query(`
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'public' 
                    AND table_type = 'BASE TABLE'
                    ORDER BY table_name
                `);
                await client.end();
                return result.rows.map(row => row.table_name);
            } catch (error) {
                console.error('Failed to get PostgreSQL tables:', error.message);
                throw new Error(`Failed to get tables: ${error.message}`);
            }
        } else {
            throw new Error(`Unsupported database type: ${type}`);
        }
    }

    /**
     * Import table data
     */
    async importTable(config, tableName) {
        const { type, database } = config;

        if (type === 'mysql' || type === 'mariadb') {
            let connection;
            try {
                connection = await this._createMySQLConnection(config);
                const [columns] = await connection.query(
                    'SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION',
                    [database, tableName]
                );
                const [rows] = await connection.query(`SELECT * FROM \`${tableName}\``);
                const headers = columns.map(col => col.COLUMN_NAME);
                const data = [headers, ...rows.map(row => headers.map(header => row[header]))];

                return {
                    tableName,
                    columns: columns.map(col => ({
                        name: col.COLUMN_NAME,
                        type: col.DATA_TYPE
                    })),
                    data,
                    rowCount: rows.length
                };
            } catch (error) {
                console.error('Failed to import MySQL table:', error.message);
                throw new Error(`Failed to import table: ${error.message}`);
            } finally {
                if (connection) await connection.end();
            }
        } else if (type === 'postgresql') {
            try {
                const client = await this._connectPostgreSQL(config);
                const columnsResult = await client.query(
                    `SELECT column_name, data_type 
                     FROM information_schema.columns 
                     WHERE table_schema = 'public' 
                     AND table_name = $1 
                     ORDER BY ordinal_position`,
                    [tableName]
                );
                const dataResult = await client.query(`SELECT * FROM "${tableName}"`);
                await client.end();

                const headers = columnsResult.rows.map(col => col.column_name);
                const data = [headers, ...dataResult.rows.map(row => headers.map(header => row[header]))];

                return {
                    tableName,
                    columns: columnsResult.rows.map(col => ({
                        name: col.column_name,
                        type: col.data_type
                    })),
                    data,
                    rowCount: dataResult.rows.length
                };
            } catch (error) {
                console.error('Failed to import PostgreSQL table:', error.message);
                throw new Error(`Failed to import table: ${error.message}`);
            }
        } else {
            throw new Error(`Unsupported database type: ${type}`);
        }
    }
}

module.exports = new SqlDatabaseService();
