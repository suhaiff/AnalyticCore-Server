const mysql = require('mysql2/promise');

class SqlDatabaseService {
    /**
     * Test database connection
     */
    async testConnection(config) {
        const { host, port, user, password, database, type } = config;

        if (type === 'mysql' || type === 'mariadb') {
            return await this.testMySQLConnection({ host, port: port || 3306, user, password, database });
        } else if (type === 'postgresql') {
            return await this.testPostgreSQLConnection({ host, port: port || 5432, user, password, database });
        } else {
            throw new Error(`Unsupported database type: ${type}`);
        }
    }

    /**
     * Test MySQL/MariaDB connection
     */
    async testMySQLConnection(config) {
        let connection;
        try {
            connection = await mysql.createConnection({
                host: config.host,
                port: config.port,
                user: config.user,
                password: config.password,
                database: config.database,
                connectTimeout: 10000
            });

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
            if (connection) {
                await connection.end();
            }
        }
    }

    /**
     * Test PostgreSQL connection (using mysql2 won't work for PostgreSQL)
     * We'll need pg package, but for now return a helpful message
     */
    async testPostgreSQLConnection(config) {
        // PostgreSQL support requires 'pg' package
        // For now, we'll check if it's installed
        try {
            const { Client } = require('pg');
            const client = new Client({
                host: config.host,
                port: config.port,
                user: config.user,
                password: config.password,
                database: config.database,
                connectionTimeoutMillis: 10000
            });

            await client.connect();
            const result = await client.query('SELECT version()');
            await client.end();

            return {
                success: true,
                message: 'Connection successful',
                serverInfo: result.rows[0]
            };
        } catch (error) {
            if (error.code === 'MODULE_NOT_FOUND') {
                throw new Error('PostgreSQL support requires the "pg" package. Please install it: npm install pg');
            }
            console.error('PostgreSQL connection test failed:', error.message);
            throw new Error(`PostgreSQL connection failed: ${error.message}`);
        }
    }

    /**
     * Get list of tables from database
     */
    async getTables(config) {
        const { host, port, user, password, database, type } = config;

        if (type === 'mysql' || type === 'mariadb') {
            return await this.getMySQLTables({ host, port: port || 3306, user, password, database });
        } else if (type === 'postgresql') {
            return await this.getPostgreSQLTables({ host, port: port || 5432, user, password, database });
        } else {
            throw new Error(`Unsupported database type: ${type}`);
        }
    }

    /**
     * Get MySQL tables
     */
    async getMySQLTables(config) {
        let connection;
        try {
            connection = await mysql.createConnection({
                host: config.host,
                port: config.port,
                user: config.user,
                password: config.password,
                database: config.database
            });

            const [rows] = await connection.query('SHOW TABLES');
            const tableKey = `Tables_in_${config.database}`;
            const tables = rows.map(row => row[tableKey]);

            return tables;
        } catch (error) {
            console.error('Failed to get MySQL tables:', error.message);
            throw new Error(`Failed to get tables: ${error.message}`);
        } finally {
            if (connection) {
                await connection.end();
            }
        }
    }

    /**
     * Get PostgreSQL tables
     */
    async getPostgreSQLTables(config) {
        try {
            const { Client } = require('pg');
            const client = new Client({
                host: config.host,
                port: config.port,
                user: config.user,
                password: config.password,
                database: config.database
            });

            await client.connect();
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
            if (error.code === 'MODULE_NOT_FOUND') {
                throw new Error('PostgreSQL support requires the "pg" package. Please install it: npm install pg');
            }
            console.error('Failed to get PostgreSQL tables:', error.message);
            throw new Error(`Failed to get tables: ${error.message}`);
        }
    }

    /**
     * Import table data
     */
    async importTable(config, tableName) {
        const { host, port, user, password, database, type } = config;

        if (type === 'mysql' || type === 'mariadb') {
            return await this.importMySQLTable({ host, port: port || 3306, user, password, database }, tableName);
        } else if (type === 'postgresql') {
            return await this.importPostgreSQLTable({ host, port: port || 5432, user, password, database }, tableName);
        } else {
            throw new Error(`Unsupported database type: ${type}`);
        }
    }

    /**
     * Import MySQL table
     */
    async importMySQLTable(config, tableName) {
        let connection;
        try {
            connection = await mysql.createConnection({
                host: config.host,
                port: config.port,
                user: config.user,
                password: config.password,
                database: config.database
            });

            // Get column information
            const [columns] = await connection.query(
                'SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION',
                [config.database, tableName]
            );

            // Get table data
            const [rows] = await connection.query(`SELECT * FROM \`${tableName}\``);

            // Convert to array of arrays format
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
            if (connection) {
                await connection.end();
            }
        }
    }

    /**
     * Import PostgreSQL table
     */
    async importPostgreSQLTable(config, tableName) {
        try {
            const { Client } = require('pg');
            const client = new Client({
                host: config.host,
                port: config.port,
                user: config.user,
                password: config.password,
                database: config.database
            });

            await client.connect();

            // Get column information
            const columnsResult = await client.query(
                `SELECT column_name, data_type 
                 FROM information_schema.columns 
                 WHERE table_schema = 'public' 
                 AND table_name = $1 
                 ORDER BY ordinal_position`,
                [tableName]
            );

            // Get table data
            const dataResult = await client.query(`SELECT * FROM "${tableName}"`);
            await client.end();

            // Convert to array of arrays format
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
            if (error.code === 'MODULE_NOT_FOUND') {
                throw new Error('PostgreSQL support requires the "pg" package. Please install it: npm install pg');
            }
            console.error('Failed to import PostgreSQL table:', error.message);
            throw new Error(`Failed to import table: ${error.message}`);
        }
    }
}

module.exports = new SqlDatabaseService();
