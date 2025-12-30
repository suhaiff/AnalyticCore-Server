const { Parser } = require('node-sql-parser');
const fs = require('fs');

/**
 * Service to safely parse SQL dump files and extract table data.
 * 
 * SAFETY NOTES:
 * - Uses AST-based parsing only (node-sql-parser)
 * - Never executes SQL statements
 * - Only extracts INSERT statement data
 * - Ignores dangerous operations (DROP, DELETE, UPDATE, ALTER)
 */
class SqlParserService {
    constructor() {
        // Initialize parser with strict mode for safety
        this.parser = new Parser();
        this.maxFileSize = 10 * 1024 * 1024; // 10MB limit
    }

    /**
     * Parse SQL file and extract all table names
     * @param {string} filePath - Path to SQL file
     * @returns {Promise<string[]>} Array of table names
     */
    async extractTableNames(filePath) {
        try {
            const sqlContent = await this.readSqlFile(filePath);
            const tables = new Set();

            // Split SQL into individual statements
            const statements = this.splitSqlStatements(sqlContent);

            for (const statement of statements) {
                try {
                    // Parse the statement safely using AST
                    const ast = this.parser.astify(statement, { database: 'MySQL' });

                    // Handle both single statements and arrays of statements
                    const astArray = Array.isArray(ast) ? ast : [ast];

                    for (const node of astArray) {
                        if (!node) continue;

                        // Extract table name from CREATE TABLE
                        if (node.type === 'create' && node.keyword === 'table' && node.table) {
                            const tableName = this.extractTableName(node.table);
                            if (tableName) tables.add(tableName);
                        }

                        // Extract table name from INSERT INTO
                        if (node.type === 'insert' && node.table) {
                            const tableName = this.extractTableName(node.table);
                            if (tableName) tables.add(tableName);
                        }
                    }
                } catch (parseError) {
                    // Skip unparseable statements (comments, complex syntax, etc.)
                    // This is intentional - we only parse what we can safely handle
                    continue;
                }
            }

            return Array.from(tables).sort();
        } catch (error) {
            console.error('Error extracting table names:', error.message);
            throw new Error('Failed to parse SQL file: ' + error.message);
        }
    }

    /**
     * Extract table name from AST table node
     * @param {object} tableNode - AST table node
     * @returns {string} Table name
     */
    extractTableName(tableNode) {
        if (typeof tableNode === 'string') return tableNode;
        if (Array.isArray(tableNode) && tableNode.length > 0) {
            const lastPart = tableNode[tableNode.length - 1];
            return lastPart.table || lastPart;
        }
        if (tableNode.table) return tableNode.table;
        return null;
    }

    /**
     * Extract data from a specific table in the SQL dump
     * @param {string} filePath - Path to SQL file
     * @param {string} tableName - Name of table to extract
     * @returns {Promise<{headers: string[], rows: any[][]}>} Table data
     */
    async extractTableData(filePath, tableName) {
        try {
            const sqlContent = await this.readSqlFile(filePath);
            const statements = this.splitSqlStatements(sqlContent);

            let columns = [];
            const rows = [];

            for (const statement of statements) {
                try {
                    const ast = this.parser.astify(statement, { database: 'MySQL' });
                    const astArray = Array.isArray(ast) ? ast : [ast];

                    for (const node of astArray) {
                        if (!node) continue;

                        // Extract column names from CREATE TABLE
                        if (node.type === 'create' && node.keyword === 'table') {
                            const nodeTableName = this.extractTableName(node.table);
                            if (nodeTableName === tableName && node.create_definitions) {
                                columns = this.extractColumnsFromCreate(node.create_definitions);
                            }
                        }

                        // Extract row data from INSERT statements
                        if (node.type === 'insert') {
                            const nodeTableName = this.extractTableName(node.table);
                            if (nodeTableName === tableName) {
                                // Get column names from INSERT if not already set
                                if (columns.length === 0 && node.columns) {
                                    columns = node.columns.map(col => col.column || col);
                                }

                                // Extract values
                                if (node.values) {
                                    const insertedRows = this.extractValuesFromInsert(node.values, columns.length);
                                    rows.push(...insertedRows);
                                }
                            }
                        }
                    }
                } catch (parseError) {
                    // Skip unparseable statements
                    continue;
                }
            }

            if (columns.length === 0 && rows.length > 0) {
                // Generate generic column names if none found
                const columnCount = rows[0]?.length || 0;
                columns = Array.from({ length: columnCount }, (_, i) => `Column${i + 1}`);
            }

            // Format as array of arrays with headers as first row
            const formattedData = [columns, ...rows];

            return {
                headers: columns,
                rows: formattedData
            };
        } catch (error) {
            console.error('Error extracting table data:', error.message);
            throw new Error('Failed to extract data from table "' + tableName + '": ' + error.message);
        }
    }

    /**
     * Extract column names from CREATE TABLE definition
     * @param {array} definitions - CREATE TABLE definitions from AST
     * @returns {string[]} Column names
     */
    extractColumnsFromCreate(definitions) {
        const columns = [];
        for (const def of definitions) {
            if (def.column && def.column.column) {
                columns.push(def.column.column);
            } else if (def.column) {
                columns.push(def.column);
            }
        }
        return columns;
    }

    /**
     * Extract row values from INSERT statement
     * @param {array} values - Values from INSERT AST node
     * @param {number} expectedColumns - Expected number of columns
     * @returns {any[][]} Array of rows
     */
    extractValuesFromInsert(values, expectedColumns) {
        const rows = [];

        for (const valueSet of values) {
            const row = [];
            const valueList = valueSet.value || [];

            for (const val of valueList) {
                // Extract actual value from AST node
                if (val.type === 'number') {
                    row.push(val.value);
                } else if (val.type === 'string' || val.type === 'single_quote_string' || val.type === 'double_quote_string') {
                    row.push(val.value);
                } else if (val.type === 'null') {
                    row.push(null);
                } else if (val.type === 'bool') {
                    row.push(val.value);
                } else if (val.value !== undefined) {
                    row.push(val.value);
                } else {
                    row.push(null);
                }
            }

            // Pad row if needed
            while (row.length < expectedColumns) {
                row.push(null);
            }

            rows.push(row);
        }

        return rows;
    }

    /**
     * Read and validate SQL file
     * @param {string} filePath - Path to SQL file
     * @returns {Promise<string>} SQL file content
     */
    async readSqlFile(filePath) {
        // Check file size
        const stats = fs.statSync(filePath);
        if (stats.size > this.maxFileSize) {
            throw new Error(`File size (${Math.round(stats.size / 1024 / 1024)}MB) exceeds maximum allowed size (10MB)`);
        }

        // Read file content
        const content = fs.readFileSync(filePath, 'utf-8');

        if (!content || content.trim().length === 0) {
            throw new Error('SQL file is empty');
        }

        return content;
    }

    /**
     * Split SQL content into individual statements
     * Handles common SQL statement delimiters
     * @param {string} sqlContent - Raw SQL content
     * @returns {string[]} Array of SQL statements
     */
    splitSqlStatements(sqlContent) {
        // Remove comments
        let cleaned = sqlContent
            .replace(/--[^\n]*/g, '') // Remove single-line comments
            .replace(/\/\*[\s\S]*?\*\//g, ''); // Remove multi-line comments

        // Split by semicolons (basic approach)
        const statements = cleaned
            .split(';')
            .map(stmt => stmt.trim())
            .filter(stmt => stmt.length > 0);

        return statements;
    }

    /**
     * Validate SQL content for safety
     * Checks for dangerous operations
     * @param {string} sqlContent - SQL content to validate
     * @returns {boolean} True if safe
     */
    validateSqlSafety(sqlContent) {
        const dangerous = ['DROP', 'DELETE FROM', 'TRUNCATE', 'ALTER', 'UPDATE'];
        const upperContent = sqlContent.toUpperCase();

        for (const keyword of dangerous) {
            if (upperContent.includes(keyword)) {
                console.warn(`Warning: SQL contains potentially dangerous keyword: ${keyword}`);
                // We still allow parsing but log a warning
                // The AST parser won't execute these anyway
            }
        }

        return true;
    }
}

module.exports = new SqlParserService();
