const express = require('express');
const router = express.Router();
const dwService = require('./dataWarehouseService');
const supabaseService = require('./supabaseService');
const XLSX = require('xlsx');
const path = require('path');
const fs = require('fs');

router.post('/test', async (req, res) => {
    try {
        const { engine, config } = req.body;
        const result = await dwService.testConnection(engine, config);
        res.json(result);
    } catch (error) {
        res.status(400).json({ error: error.message });
    }
});

router.post('/tables', async (req, res) => {
    try {
        const { engine, config } = req.body;
        const tables = await dwService.getTables(engine, config);
        res.json({ tables });
    } catch (error) {
        res.status(400).json({ error: error.message });
    }
});

router.post('/import', async (req, res) => {
    try {
        const { userId, engine, config, tableNames, title } = req.body;
        // 1. Create file record in Supabase once (represents the connection/import set)
        const sourceInfo = {
            type: 'data_warehouse',
            tables: tableNames, // Store all tables in metadata
            engine: engine,
            config: config,
            refreshMode: 'manual',
            lastRefresh: new Date().toISOString()
        };

        const fileId = await supabaseService.createFile(
            parseInt(userId),
            title || `${engine.toUpperCase()}_Import`,
            'application/json',
            0, // Size
            tableNames.length, // Total sheets
            sourceInfo
        );

        // 2. Process each table
        const importedTables = [];
        for (let i = 0; i < tableNames.length; i++) {
            const tableName = tableNames[i];
            const result = await dwService.importTable(engine, config, tableName);
            
            if (result.data && result.data.length > 0) {
                const columnCount = result.data[0].length;
                
                // Create sheet for this table
                const sheetId = await supabaseService.createSheet(
                    fileId,
                    tableName,
                    i,
                    result.data.length,
                    columnCount
                );

                // Insert row data in batches
                const batchSize = 500;
                for (let j = 0; j < result.data.length; j += batchSize) {
                    const batchData = result.data.slice(j, j + batchSize);
                    const insertRows = batchData.map((row, idx) => ({
                        rowIndex: j + idx,
                        rowData: row
                    }));
                    await supabaseService.createExcelDataBatch(sheetId, insertRows);
                }

                importedTables.push({
                    id: sheetId,
                    name: tableName,
                    data: result.data,
                    fileId: fileId
                });
            }
        }

        res.json({ tables: importedTables, title: title || `${engine.toUpperCase()} Import` });

    } catch (error) {
        res.status(400).json({ error: error.message });
    }
});

module.exports = router;
