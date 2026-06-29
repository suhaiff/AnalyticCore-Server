const { BigQuery } = require('@google-cloud/bigquery');
const snowflake = require('snowflake-sdk');
const { DBSQLClient } = require('@databricks/sql');
const { MongoClient } = require('mongodb');
const sql = require('mssql');
const { Client: PgClient } = require('pg');

class DataWarehouseService {

    // ==========================================
    // BIGQUERY
    // ==========================================
    async _getBigQueryClient(config) {
        let creds = typeof config.credentials === 'string' ? JSON.parse(config.credentials) : config.credentials;
        return new BigQuery({ projectId: creds.project_id, credentials: creds });
    }

    async testBigQuery(config) {
        const bigquery = await this._getBigQueryClient(config);
        await bigquery.getDatasets({ maxResults: 1 });
        return { success: true };
    }

    async getBigQueryTables(config) {
        const bigquery = await this._getBigQueryClient(config);
        const dataset = bigquery.dataset(config.dataset);
        const [tables] = await dataset.getTables();
        return tables.map(t => t.id);
    }

    async importBigQueryTable(config, tableName) {
        const bigquery = await this._getBigQueryClient(config);
        const query = `SELECT * FROM \`${config.dataset}.${tableName}\` LIMIT 10000`;
        const [job] = await bigquery.createQueryJob({ query });
        const [rows] = await job.getQueryResults();
        
        if (rows.length === 0) return { tableName, columns: [], data: [], rowCount: 0 };
        
        const headers = Object.keys(rows[0]);
        const data = [headers, ...rows.map(r => headers.map(h => r[h]))];
        
        return { tableName, columns: headers.map(h => ({ name: h, type: 'string' })), data, rowCount: rows.length };
    }

    // ==========================================
    // SNOWFLAKE
    // ==========================================
    _getSnowflakeConnection(config) {
        return new Promise((resolve, reject) => {
            const timeoutId = setTimeout(() => {
                reject(new Error('Connection timed out. Please check your account identifier and network connection.'));
            }, 15000);

            const connection = snowflake.createConnection({
                account: config.account,
                username: config.username,
                password: config.password,
                warehouse: config.warehouse,
                database: config.database,
                schema: config.schema || 'PUBLIC',
                timeout: 10000,
                clientSessionKeepAlive: false
            });
            connection.connect((err, conn) => {
                clearTimeout(timeoutId);
                if (err) reject(err);
                else resolve(conn);
            });
        });
    }

    _runSnowflakeQuery(connection, query) {
        return new Promise((resolve, reject) => {
            connection.execute({
                sqlText: query,
                complete: (err, stmt, rows) => {
                    if (err) reject(err);
                    else resolve(rows);
                }
            });
        });
    }

    async testSnowflake(config) {
        const conn = await this._getSnowflakeConnection(config);
        await this._runSnowflakeQuery(conn, 'SELECT 1');
        return { success: true };
    }

    async getSnowflakeTables(config) {
        const conn = await this._getSnowflakeConnection(config);
        const rows = await this._runSnowflakeQuery(conn, `SHOW TABLES IN SCHEMA ${config.schema || 'PUBLIC'}`);
        return rows.map(r => r.name);
    }

    async importSnowflakeTable(config, tableName) {
        const conn = await this._getSnowflakeConnection(config);
        const rows = await this._runSnowflakeQuery(conn, `SELECT * FROM "${tableName}" LIMIT 10000`);
        
        if (rows.length === 0) return { tableName, columns: [], data: [], rowCount: 0 };
        
        const headers = Object.keys(rows[0]);
        const data = [headers, ...rows.map(r => headers.map(h => r[h]))];
        
        return { tableName, columns: headers.map(h => ({ name: h, type: 'string' })), data, rowCount: rows.length };
    }

    // ==========================================
    // AZURE SQL
    // ==========================================
    async _getAzureSqlConnection(config) {
        return await sql.connect({
            user: config.user,
            password: config.password,
            server: config.server,
            database: config.database,
            options: { encrypt: true, trustServerCertificate: false }
        });
    }

    async testAzureSql(config) {
        await this._getAzureSqlConnection(config);
        return { success: true };
    }

    async getAzureSqlTables(config) {
        const pool = await this._getAzureSqlConnection(config);
        const result = await pool.request().query("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'");
        return result.recordset.map(r => r.TABLE_NAME);
    }

    async importAzureSqlTable(config, tableName) {
        const pool = await this._getAzureSqlConnection(config);
        const result = await pool.request().query(`SELECT TOP 10000 * FROM [${tableName}]`);
        const rows = result.recordset;
        
        if (rows.length === 0) return { tableName, columns: [], data: [], rowCount: 0 };
        
        const headers = Object.keys(rows[0]);
        const data = [headers, ...rows.map(r => headers.map(h => r[h]))];
        
        return { tableName, columns: headers.map(h => ({ name: h, type: 'string' })), data, rowCount: rows.length };
    }

    // ==========================================
    // DATABRICKS
    // ==========================================
    async _getDatabricksClient(config) {
        const client = new DBSQLClient();
        await client.connect({
            token: config.token,
            host: config.host,
            path: config.path
        });
        return client;
    }

    async testDatabricks(config) {
        const client = await this._getDatabricksClient(config);
        const session = await client.openSession();
        await session.executeStatement('SELECT 1', { runAsync: true });
        await session.close();
        await client.close();
        return { success: true };
    }

    async getDatabricksTables(config) {
        const client = await this._getDatabricksClient(config);
        const session = await client.openSession();
        const operation = await session.executeStatement(`SHOW TABLES IN ${config.catalog}.${config.schema}`, { runAsync: true });
        const result = await operation.fetchAll();
        await session.close();
        await client.close();
        return result.map(r => r.tableName);
    }

    async importDatabricksTable(config, tableName) {
        const client = await this._getDatabricksClient(config);
        const session = await client.openSession();
        const operation = await session.executeStatement(`SELECT * FROM ${config.catalog}.${config.schema}.${tableName} LIMIT 10000`, { runAsync: true });
        const rows = await operation.fetchAll();
        await session.close();
        await client.close();
        
        if (rows.length === 0) return { tableName, columns: [], data: [], rowCount: 0 };
        
        const headers = Object.keys(rows[0]);
        const data = [headers, ...rows.map(r => headers.map(h => r[h]))];
        
        return { tableName, columns: headers.map(h => ({ name: h, type: 'string' })), data, rowCount: rows.length };
    }

    // ==========================================
    // REDSHIFT (using pg)
    // ==========================================
    async _getRedshiftClient(config) {
        const client = new PgClient({
            host: config.host,
            port: config.port || 5439,
            user: config.user,
            password: config.password,
            database: config.database,
            ssl: { rejectUnauthorized: false }
        });
        await client.connect();
        return client;
    }

    async testRedshift(config) {
        const client = await this._getRedshiftClient(config);
        await client.query('SELECT 1');
        await client.end();
        return { success: true };
    }

    async getRedshiftTables(config) {
        const client = await this._getRedshiftClient(config);
        const result = await client.query("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'");
        await client.end();
        return result.rows.map(r => r.table_name);
    }

    async importRedshiftTable(config, tableName) {
        const client = await this._getRedshiftClient(config);
        const result = await client.query(`SELECT * FROM "${tableName}" LIMIT 10000`);
        await client.end();
        const rows = result.rows;
        
        if (rows.length === 0) return { tableName, columns: [], data: [], rowCount: 0 };
        
        const headers = Object.keys(rows[0]);
        const data = [headers, ...rows.map(r => headers.map(h => r[h]))];
        
        return { tableName, columns: headers.map(h => ({ name: h, type: 'string' })), data, rowCount: rows.length };
    }

    // ==========================================
    // MONGODB
    // ==========================================
    async _getMongoClient(config) {
        const client = new MongoClient(config.connectionString);
        await client.connect();
        return client;
    }

    async testMongoDb(config) {
        const client = await this._getMongoClient(config);
        await client.db().admin().ping();
        await client.close();
        return { success: true };
    }

    async getMongoDbTables(config) {
        const client = await this._getMongoClient(config);
        const collections = await client.db(config.database).listCollections().toArray();
        await client.close();
        return collections.map(c => c.name);
    }

    async importMongoDbTable(config, tableName) {
        const client = await this._getMongoClient(config);
        const docs = await client.db(config.database).collection(tableName).find().limit(10000).toArray();
        await client.close();
        
        if (docs.length === 0) return { tableName, columns: [], data: [], rowCount: 0 };
        
        // Flatten simple keys for MongoDB documents to fake a 2D array
        const allKeys = new Set();
        docs.forEach(doc => Object.keys(doc).forEach(k => allKeys.add(k)));
        const headers = Array.from(allKeys);
        
        const data = [headers, ...docs.map(doc => headers.map(h => {
            const val = doc[h];
            if (typeof val === 'object' && val !== null) return JSON.stringify(val);
            return val;
        }))];
        
        return { tableName, columns: headers.map(h => ({ name: h, type: 'string' })), data, rowCount: docs.length };
    }

    // ==========================================
    // DISPATCHER
    // ==========================================
    async testConnection(engine, config) {
        switch(engine) {
            case 'bigquery': return await this.testBigQuery(config);
            case 'snowflake': return await this.testSnowflake(config);
            case 'azuresql': return await this.testAzureSql(config);
            case 'databricks': return await this.testDatabricks(config);
            case 'redshift': return await this.testRedshift(config);
            case 'mongodb': return await this.testMongoDb(config);
            default: throw new Error(`Unknown engine: ${engine}`);
        }
    }

    async getTables(engine, config) {
        switch(engine) {
            case 'bigquery': return await this.getBigQueryTables(config);
            case 'snowflake': return await this.getSnowflakeTables(config);
            case 'azuresql': return await this.getAzureSqlTables(config);
            case 'databricks': return await this.getDatabricksTables(config);
            case 'redshift': return await this.getRedshiftTables(config);
            case 'mongodb': return await this.getMongoDbTables(config);
            default: throw new Error(`Unknown engine: ${engine}`);
        }
    }

    async importTable(engine, config, tableName) {
        switch(engine) {
            case 'bigquery': return await this.importBigQueryTable(config, tableName);
            case 'snowflake': return await this.importSnowflakeTable(config, tableName);
            case 'azuresql': return await this.importAzureSqlTable(config, tableName);
            case 'databricks': return await this.importDatabricksTable(config, tableName);
            case 'redshift': return await this.importRedshiftTable(config, tableName);
            case 'mongodb': return await this.importMongoDbTable(config, tableName);
            default: throw new Error(`Unknown engine: ${engine}`);
        }
    }
}

module.exports = new DataWarehouseService();
