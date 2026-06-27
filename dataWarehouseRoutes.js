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
        const importedTables = [];

        for (const tableName of tableNames) {
            const result = await dwService.importTable(engine, config, tableName);
            importedTables.push(result);
        }
        
        // Emulate the SQL Database saving behavior to Supabase
        const uploadsDir = path.join(__dirname, 'uploads');
        if (!fs.existsSync(uploadsDir)) fs.mkdirSync(uploadsDir);
        
        const fileId = Date.now().toString() + '_' + Math.floor(Math.random() * 1000);
        const fileName = `${title || engine}_${fileId}.sql_dump`;
        const filePath = path.join(uploadsDir, fileName);
        
        // Write a dummy file to pass the upload check
        fs.writeFileSync(filePath, JSON.stringify(importedTables));

        await supabaseService.uploadSqlFile(
            userId,
            fileName,
            fileName,
            filePath,
            'application/json',
            fs.statSync(filePath).size
        );
        
        // Save table contents logic (simulated for now, similar to index.js sql upload flow)
        // Here we just return the processed tables for frontend to render directly
        res.json({ tables: importedTables, title: title || `${engine} Import` });

    } catch (error) {
        res.status(400).json({ error: error.message });
    }
});

module.exports = router;
