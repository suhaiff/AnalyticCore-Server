const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const fs = require('fs');
const XLSX = require('xlsx');
const dataverseService = require('./dataverseService');

const app = express();
const port = 3001;

app.use(cors());
app.use(bodyParser.json());

// Ensure uploads directory exists
const uploadDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadDir)) {
    fs.mkdirSync(uploadDir);
}

// Configure Multer
const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, uploadDir)
    },
    filename: function (req, file, cb) {
        const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9)
        cb(null, uniqueSuffix + '-' + file.originalname)
    }
})

const upload = multer({ storage: storage });

console.log('Starting server with Dataverse integration...');

// Auth Endpoints
app.post('/api/login', async (req, res) => {
    try {
        const { email, password } = req.body;
        const user = await dataverseService.getUserByEmail(email);

        if (user && user.password === password) {
            // Don't send password back
            const { password, ...userWithoutPassword } = user;
            res.json(userWithoutPassword);
        } else {
            res.status(401).json({ error: 'Invalid credentials' });
        }
    } catch (error) {
        console.error('Login error:', error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/signup', async (req, res) => {
    try {
        const { name, email, password } = req.body;

        // Check if user already exists
        const existingUser = await dataverseService.getUserByEmail(email);
        if (existingUser) {
            return res.status(400).json({ error: 'Email already exists' });
        }

        const newUser = await dataverseService.createUser(name, email, password, 'USER');
        res.json(newUser);
    } catch (error) {
        console.error('Signup error:', error);
        if (error.response?.status === 400) {
            res.status(400).json({ error: 'Email already exists' });
        } else {
            res.status(500).json({ error: error.message });
        }
    }
});

// User Management (Admin)
app.get('/api/users', async (req, res) => {
    try {
        const users = await dataverseService.getUsers();
        res.json(users);
    } catch (error) {
        console.error('Get users error:', error);
        res.status(500).json({ error: error.message });
    }
});

app.delete('/api/users/:id', async (req, res) => {
    try {
        const userId = parseInt(req.params.id);
        await dataverseService.deleteUser(userId);
        res.json({ message: 'User deleted successfully' });
    } catch (error) {
        console.error('Delete user error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Dashboard Endpoints
app.post('/api/dashboards', async (req, res) => {
    try {
        const { userId, dashboard } = req.body;

        if (!dashboard) {
            return res.status(400).json({ error: 'Missing dashboard data' });
        }

        const { name, dataModel, chartConfigs } = dashboard;
        const result = await dataverseService.createDashboard(userId, name, dataModel, chartConfigs);
        res.json(result);
    } catch (error) {
        console.error('Save dashboard error:', error);
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/dashboards', async (req, res) => {
    try {
        const userId = parseInt(req.query.userId);
        const dashboards = await dataverseService.getDashboardsByUser(userId);
        res.json(dashboards);
    } catch (error) {
        console.error('Get dashboards error:', error);
        res.status(500).json({ error: error.message });
    }
});

app.delete('/api/dashboards/:id', async (req, res) => {
    try {
        const id = parseInt(req.params.id);
        await dataverseService.deleteDashboard(id);
        res.json({ message: 'Dashboard deleted' });
    } catch (error) {
        console.error('Delete dashboard error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Admin: Get All Dashboards
app.get('/api/admin/dashboards', async (req, res) => {
    try {
        const dashboards = await dataverseService.getAllDashboards();
        res.json(dashboards);
    } catch (error) {
        console.error('Get all dashboards error:', error);
        res.status(500).json({ error: error.message });
    }
});

// File Upload Endpoints
app.post('/api/upload', upload.single('file'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }

    const { userId } = req.body;
    const { filename, originalname, path: filePath, mimetype, size } = req.file;

    try {
        // Check if it's an Excel file
        const isExcelFile = mimetype.includes('spreadsheet') || mimetype.includes('excel') ||
            filename.endsWith('.xlsx') || filename.endsWith('.xls');

        if (!isExcelFile) {
            return res.status(400).json({ error: 'Only Excel files are supported' });
        }

        // Read the Excel file
        const uploadedWorkbook = XLSX.readFile(filePath);
        const sheetNames = uploadedWorkbook.SheetNames;
        const sheetCount = sheetNames.length;

        console.log(`Processing file: ${originalname} with ${sheetCount} sheets`);

        // 1. Create file record in Dataverse
        const fileId = await dataverseService.createFile(
            parseInt(userId),
            originalname,
            mimetype,
            size,
            sheetCount
        );

        console.log(`Created file record with ID: ${fileId}`);

        // 2. Process each sheet
        for (let i = 0; i < sheetNames.length; i++) {
            const sheetName = sheetNames[i];
            const worksheet = uploadedWorkbook.Sheets[sheetName];

            // Convert sheet to array of arrays
            const sheetData = XLSX.utils.sheet_to_json(worksheet, { header: 1, defval: null });
            const rowCount = sheetData.length;
            const columnCount = rowCount > 0 ? Math.max(...sheetData.map(row => row.length)) : 0;

            console.log(`Processing sheet "${sheetName}": ${rowCount} rows, ${columnCount} columns`);

            // Create sheet record
            const sheetId = await dataverseService.createSheet(
                fileId,
                sheetName,
                i,
                rowCount,
                columnCount
            );

            console.log(`Created sheet record with ID: ${sheetId}`);

            // Insert row data - we need to batch this due to Dataverse API limits
            // Process in smaller batches to avoid timeouts
            const batchSize = 100;
            for (let rowIndex = 0; rowIndex < sheetData.length; rowIndex++) {
                const rowData = sheetData[rowIndex];
                await dataverseService.createExcelData(sheetId, rowIndex, rowData);

                // Log progress for large files
                if ((rowIndex + 1) % batchSize === 0) {
                    console.log(`  Inserted ${rowIndex + 1}/${sheetData.length} rows for sheet "${sheetName}"`);
                }
            }

            console.log(`Completed inserting all rows for sheet "${sheetName}"`);
        }

        // 3. Log to file_upload_log
        const now = new Date();
        await dataverseService.createFileUploadLog(
            fileId,
            now.toISOString().split('T')[0],
            now.toTimeString().split(' ')[0],
            filePath,
            'SUCCESS'
        );

        console.log('Created upload log entry');

        // 4. Legacy Excel logging (optional - keep for backward compatibility)
        try {
            const logFilePath = path.join(__dirname, '..', 'user file log.xlsx');
            let workbook;

            if (fs.existsSync(logFilePath)) {
                workbook = XLSX.readFile(logFilePath);
            } else {
                workbook = XLSX.utils.book_new();
            }

            let uploadsData = [];
            if (workbook.SheetNames.includes('Uploads')) {
                uploadsData = XLSX.utils.sheet_to_json(workbook.Sheets['Uploads']);
            }

            const newUploadRow = {
                'S.No': uploadsData.length + 1,
                'Path': 'Dataverse',
                'File Type': mimetype,
                'File name': originalname,
                'Uploaded date': now.toLocaleDateString(),
                'Uploaded time': now.toLocaleTimeString()
            };
            uploadsData.push(newUploadRow);

            const uploadsSheet = XLSX.utils.json_to_sheet(uploadsData);
            if (workbook.SheetNames.includes('Uploads')) {
                workbook.Sheets['Uploads'] = uploadsSheet;
            } else {
                XLSX.utils.book_append_sheet(workbook, uploadsSheet, 'Uploads');
            }

            let fileDetailsData = [];
            if (workbook.SheetNames.includes('File Details')) {
                fileDetailsData = XLSX.utils.sheet_to_json(workbook.Sheets['File Details']);
            }

            sheetNames.forEach(sheetName => {
                const newDetailRow = {
                    'S.No': fileDetailsData.length + 1,
                    'Path': 'Dataverse',
                    'File name': originalname,
                    'Sheet count': sheetCount,
                    'Sheet Name': sheetName
                };
                fileDetailsData.push(newDetailRow);
            });

            const fileDetailsSheet = XLSX.utils.json_to_sheet(fileDetailsData);
            if (workbook.SheetNames.includes('File Details')) {
                workbook.Sheets['File Details'] = fileDetailsSheet;
            } else {
                XLSX.utils.book_append_sheet(workbook, fileDetailsSheet, 'File Details');
            }

            XLSX.writeFile(workbook, logFilePath);
            console.log('Logged upload to Excel:', logFilePath);
        } catch (logErr) {
            console.error("Error logging to Excel:", logErr);
        }

        // Delete the physical file after successful database storage
        try {
            fs.unlinkSync(filePath);
            console.log('Deleted physical file:', filePath);
        } catch (deleteErr) {
            console.error('Error deleting file:', deleteErr);
            // Don't fail the request if file deletion fails
        }

        res.json({
            message: 'File uploaded and data stored successfully in Dataverse',
            file: {
                id: fileId,
                originalName: originalname,
                sheetCount: sheetCount,
                sheets: sheetNames
            }
        });

    } catch (err) {
        console.error("Upload Error:", err);

        // Try to log the error
        try {
            const now = new Date();
            await dataverseService.createFileUploadLog(
                0, // fileId might not exist yet
                now.toISOString().split('T')[0],
                now.toTimeString().split(' ')[0],
                filePath,
                'FAILED',
                err.message
            );
        } catch (logError) {
            console.error('Error logging failure:', logError);
        }

        res.status(500).json({ error: 'Failed to upload file: ' + err.message });
    }
});

// Log Configuration Endpoint
app.post('/api/log-config', async (req, res) => {
    try {
        const { fileName, columns, joinConfigs } = req.body;

        // Log to Dataverse
        await dataverseService.createDataConfigLog(fileName, columns, joinConfigs);

        // Also log to Excel file for backward compatibility
        const logFilePath = path.join(__dirname, '..', 'user file log.xlsx');
        let workbook;

        if (fs.existsSync(logFilePath)) {
            workbook = XLSX.readFile(logFilePath);
        } else {
            workbook = XLSX.utils.book_new();
        }

        const now = new Date();
        let configData = [];

        if (workbook.SheetNames.includes('Configuration Logs')) {
            configData = XLSX.utils.sheet_to_json(workbook.Sheets['Configuration Logs']);
        }

        const joinConfigString = (joinConfigs && joinConfigs.length > 0)
            ? joinConfigs.map(j => `${j.leftTableId}.${j.leftKey} ${j.type} JOIN ${j.rightTableId}.${j.rightKey}`).join('; ')
            : "no join configs";

        const newRow = {
            'S.No': configData.length + 1,
            'File Name': fileName,
            'Date': now.toLocaleDateString(),
            'Time': now.toLocaleTimeString(),
            'Columns': Array.isArray(columns) ? columns.join(', ') : columns,
            'Join Configs': joinConfigString
        };

        configData.push(newRow);
        const newSheet = XLSX.utils.json_to_sheet(configData);

        if (workbook.SheetNames.includes('Configuration Logs')) {
            workbook.Sheets['Configuration Logs'] = newSheet;
        } else {
            XLSX.utils.book_append_sheet(workbook, newSheet, 'Configuration Logs');
        }

        XLSX.writeFile(workbook, logFilePath);
        console.log('Logged configuration to Excel and Dataverse');
        res.json({ message: 'Configuration logged successfully' });

    } catch (err) {
        console.error("Error logging configuration:", err);
        res.status(500).json({ error: 'Failed to log configuration' });
    }
});

// Admin: Get All Uploads
app.get('/api/admin/uploads', async (req, res) => {
    try {
        const uploads = await dataverseService.getAllUploads();
        res.json(uploads);
    } catch (error) {
        console.error('Get all uploads error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Get File Content (for Preview) - Now reads from Dataverse
app.get('/api/uploads/:id/content', async (req, res) => {
    try {
        const id = parseInt(req.params.id);
        const fileContent = await dataverseService.getFileContent(id);

        console.log(`Retrieved file content with ${fileContent.sheets.length} sheets`);
        res.json(fileContent);
    } catch (error) {
        console.error('Get file content error:', error);
        if (error.message === 'File not found') {
            res.status(404).json({ error: 'File not found' });
        } else {
            res.status(500).json({ error: 'Failed to retrieve file content' });
        }
    }
});

app.listen(port, () => {
    console.log(`Server running on port ${port} with Microsoft Dataverse integration`);
    console.log('Dataverse Configuration:');
    console.log(`  Base URL: ${dataverseService.baseUrl}`);
    console.log(`  API Path: ${dataverseService.apiPath}`);
});
