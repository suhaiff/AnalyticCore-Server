// SQL Dump Endpoints

// Metadata endpoint - Extract table names from SQL dump
app.post('/api/sql/metadata', async (req, res) => {
    try {
        const { fileId } = req.body;
        if (!fileId) return res.status(400).json({ error: 'Missing file ID' });

        // Get file info from database
        const file = await supabaseService.getFileById(fileId);
        if (!file) return res.status(404).json({ error: 'File not found' });

        // Get the uploaded file path from uploads directory
        const uploadsDir = path.join(__dirname, 'uploads');
        const files = fs.readdirSync(uploadsDir);
        const sqlFile = files.find(f => f.includes(file.original_name) || f.endsWith(file.original_name));

        if (!sqlFile) {
            return res.status(404).json({ error: 'SQL file not found on server. Please re-upload.' });
        }

        const filePath = path.join(uploadsDir, sqlFile);

        console.log(`Extracting metadata from SQL file: ${filePath}`);

        // Extract table names using SQL parser service (safe, no execution)
        const tables = await sqlParserService.extractTableNames(filePath);

        if (!tables || tables.length === 0) {
            return res.status(400).json({ error: 'No tables found in SQL dump. Please ensure the file contains CREATE TABLE or INSERT INTO statements.' });
        }

        res.json({ fileId, tables });
    } catch (error) {
        console.error('SQL metadata error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// Import endpoint - Extract and import specific table from SQL dump
app.post('/api/sql/import', async (req, res) => {
    try {
        const { userId, fileId, table, title } = req.body;

        if (!userId || !fileId || !table) {
            return res.status(400).json({ error: 'Missing required parameters (userId, fileId, table)' });
        }

        console.log(`Importing SQL table: ${table} for user ${userId}`);

        // Get file info
        const file = await supabaseService.getFileById(fileId);
        if (!file) return res.status(404).json({ error: 'File not found' });

        // Get the uploaded file path
        const uploadsDir = path.join(__dirname, 'uploads');
        const files = fs.readdirSync(uploadsDir);
        const sqlFile = files.find(f => f.includes(file.original_name) || f.endsWith(file.original_name));

        if (!sqlFile) {
            return res.status(404).json({ error: 'SQL file not found on server. Please re-upload.' });
        }

        const filePath = path.join(uploadsDir, sqlFile);

        // Extract table data using SQL parser service (safe, no execution)
        const result = await sqlParserService.extractTableData(filePath, table);
        const { headers, rows } = result;

        if (!rows || rows.length === 0) {
            return res.status(400).json({ error: 'The selected table is empty or has no data.' });
        }

        const rowCount = rows.length - 1; // Subtract header row
        const columnCount = headers.length;

        // Create a new file record for this specific table import
        const sourceInfo = {
            type: 'sql_dump',
            table: table,
            originalFileId: fileId,
            refreshMode: 'static' // SQL dumps are static snapshots
        };

        const importedFileId = await supabaseService.createFile(
            parseInt(userId),
            title || `SQL: ${table}`,
            'application/sql',
            0,
            1, // One sheet per table
            sourceInfo
        );

        // Create sheet record
        const sheetId = await supabaseService.createSheet(
            importedFileId,
            table,
            0,
            rowCount,
            columnCount
        );

        // Insert row data (skip header row at index 0)
        const batchSize = 100;
        for (let rowIndex = 1; rowIndex < rows.length; rowIndex++) {
            await supabaseService.createExcelData(sheetId, rowIndex - 1, rows[rowIndex]);
            if (rowIndex % batchSize === 0) {
                console.log(`  Inserted ${rowIndex}/${rows.length - 1} rows for SQL table "${table}"`);
            }
        }

        console.log(`SQL table "${table}" imported successfully`);

        res.json({
            message: 'SQL table imported successfully',
            fileId: importedFileId,
            tableName: table,
            rowCount,
            data: rows
        });
    } catch (error) {
        console.error('SQL import error:', error.message);
        res.status(500).json({ error: error.message });
    }
});
