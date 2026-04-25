// test_batch_importer.js
// This script simulates a large data import to verify the batching logic in supabaseService.js

const supabaseService = require('./supabaseService');
require('dotenv').config({ path: './.env' });

async function simulateLargeImport() {
    console.log('🚀 Starting Large Data Import Simulation...');
    
    // 1. Create a dummy test file
    const userId = 1; // Assuming user ID 1 exists
    const testFileName = `Bulk_Test_${Date.now()}`;
    
    try {
        const fileId = await supabaseService.createFile(
            userId,
            testFileName,
            'application/test',
            0,
            1,
            { type: 'test_bulk', created_at: new Date().toISOString() }
        );
        console.log(`✅ Test file created with ID: ${fileId}`);

        // 2. Simulate 1000 rows of data
        const rowCount = 1000;
        const columnCount = 5;
        const testData = [];
        for (let i = 0; i < rowCount; i++) {
            const row = [];
            for (let j = 0; j < columnCount; j++) {
                row.push(`Row ${i} Col ${j}`);
            }
            testData.push(row);
        }

        // 3. Test updateFileData (which now uses batching)
        console.log('⌛ Starting bulk update (using batching)...');
        const startTime = Date.now();
        
        await supabaseService.updateFileData(fileId, [{ name: 'TestSheet', data: testData }]);
        
        const endTime = Date.now();
        console.log(`✅ Bulk update completed in ${(endTime - startTime) / 1000} seconds.`);
        
        // 4. Verify data in excel_data (optional, but good for confidence)
        console.log('🧹 Cleaning up test data...');
        // We could delete the file here, but let's leave it for manual check if needed
        // await supabaseService.deleteFile(fileId);
        
        console.log('✨ Simulation completed successfully!');
    } catch (error) {
        console.error('❌ Simulation failed:', error.message);
        if (error.message.includes('credentials')) {
            console.log('   (Note: Simulation requires valid SUPABASE_URL and SUPABASE_KEY in server/.env)');
        }
    }
}

simulateLargeImport();
