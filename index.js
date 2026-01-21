const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const fs = require('fs');
const XLSX = require('xlsx');
const supabaseService = require('./supabaseService');
const googleSheetsService = require('./googleSheetsService');
const sharepointService = require('./sharepointService');
const sharepointOAuthService = require('./sharepointOAuthService');
const sqlDatabaseService = require('./sqlDatabaseService');




const app = express();
const port = process.env.PORT || 3001;

// CORS Configuration - Allow specific origins
const allowedOrigins = [
    'http://localhost:3000',
    'http://localhost:3001',
    'http://localhost:5173',  // Vite dev server
    'https://analyticcore-server.onrender.com',
    'https://analytic-core.netlify.app',  // Netlify frontend
    process.env.FRONTEND_URL,  // Environment variable for additional domains
].filter(Boolean);  // Remove undefined values

app.use(cors({
    origin: function (origin, callback) {
        // Allow requests with no origin (like mobile apps, Postman, or curl)
        if (!origin) return callback(null, true);

        if (allowedOrigins.indexOf(origin) !== -1) {
            callback(null, true);
        } else {
            console.log('CORS blocked origin:', origin);
            callback(new Error('Not allowed by CORS'));
        }
    },
    credentials: true,
    methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
    allowedHeaders: ['Content-Type', 'Authorization']
}));

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

console.log('Starting server with Supabase integration...');

// Auth Endpoints
app.post('/api/login', async (req, res) => {
    try {
        const { email, password } = req.body;
        const user = await supabaseService.getUserByEmail(email);

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
        const existingUser = await supabaseService.getUserByEmail(email);
        if (existingUser) {
            return res.status(400).json({ error: 'Email already exists' });
        }

        const newUser = await supabaseService.createUser(name, email, password, 'USER');
        res.json(newUser);
    } catch (error) {
        console.error('Signup error:', error);
        res.status(500).json({ error: error.message });
    }
});

// User Management (Admin)
app.get('/api/users', async (req, res) => {
    try {
        const users = await supabaseService.getUsers();
        res.json(users);
    } catch (error) {
        console.error('Get users error:', error);
        res.status(500).json({ error: error.message });
    }
});

app.delete('/api/users/:id', async (req, res) => {
    try {
        const userId = parseInt(req.params.id);
        await supabaseService.deleteUser(userId);
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
        const result = await supabaseService.createDashboard(userId, name, dataModel, chartConfigs);
        res.json(result);
    } catch (error) {
        console.error('Save dashboard error:', error);
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/dashboards', async (req, res) => {
    try {
        const userId = parseInt(req.query.userId);
        const dashboards = await supabaseService.getDashboardsByUser(userId);
        res.json(dashboards);
    } catch (error) {
        console.error('Get dashboards error:', error);
        res.status(500).json({ error: error.message });
    }
});

app.delete('/api/dashboards/:id', async (req, res) => {
    try {
        const id = parseInt(req.params.id);
        await supabaseService.deleteDashboard(id);
        res.json({ message: 'Dashboard deleted' });
    } catch (error) {
        console.error('Delete dashboard error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Admin: Get All Dashboards
app.get('/api/admin/dashboards', async (req, res) => {
    try {
        const dashboards = await supabaseService.getAllDashboards();
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

        // 1. Create file record in Supabase
        const fileId = await supabaseService.createFile(
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
            const sheetId = await supabaseService.createSheet(
                fileId,
                sheetName,
                i,
                rowCount,
                columnCount
            );

            console.log(`Created sheet record with ID: ${sheetId}`);

            // Insert row data - process in smaller batches to avoid timeouts
            const batchSize = 100;
            for (let rowIndex = 0; rowIndex < sheetData.length; rowIndex++) {
                const rowData = sheetData[rowIndex];
                await supabaseService.createExcelData(sheetId, rowIndex, rowData);

                // Log progress for large files
                if ((rowIndex + 1) % batchSize === 0) {
                    console.log(`  Inserted ${rowIndex + 1}/${sheetData.length} rows for sheet "${sheetName}"`);
                }
            }

            console.log(`Completed inserting all rows for sheet "${sheetName}"`);
        }

        // 3. Log to file_upload_log
        const now = new Date();
        await supabaseService.createFileUploadLog(
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
                'Path': 'Supabase',
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
                    'Path': 'Supabase',
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
            message: 'File uploaded and data stored successfully in Supabase',
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
            await supabaseService.createFileUploadLog(
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

        // Log to Supabase
        await supabaseService.createDataConfigLog(fileName, columns, joinConfigs);

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
        console.log('Logged configuration to Excel and Supabase');
        res.json({ message: 'Configuration logged successfully' });

    } catch (err) {
        console.error("Error logging configuration:", err);
        res.status(500).json({ error: 'Failed to log configuration' });
    }
});

// Admin: Get All Uploads
app.get('/api/admin/uploads', async (req, res) => {
    try {
        const uploads = await supabaseService.getAllUploads();
        res.json(uploads);
    } catch (error) {
        console.error('Get all uploads error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Google Sheets Endpoints
app.post('/api/google-sheets/metadata', async (req, res) => {
    try {
        const { url } = req.body;
        if (!url) return res.status(400).json({ error: 'Missing Google Sheet URL' });

        const spreadsheetId = googleSheetsService.extractSpreadsheetId(url);
        if (!spreadsheetId) return res.status(400).json({ error: 'Invalid Google Sheet URL' });

        const metadata = await googleSheetsService.getMetadata(spreadsheetId);
        res.json({ spreadsheetId, ...metadata });
    } catch (error) {
        console.error('Google Sheets metadata error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/google-sheets/import', async (req, res) => {
    try {
        const { userId, spreadsheetId, sheetName, range, title } = req.body;

        if (!userId || !spreadsheetId || !sheetName) {
            return res.status(400).json({ error: 'Missing required parameters' });
        }

        console.log(`Importing Google Sheet: ${spreadsheetId}, Sheet: ${sheetName}`);

        // 1. Fetch data from Google Sheets
        const data = await googleSheetsService.getSheetData(spreadsheetId, sheetName, range);
        if (!data || data.length === 0) {
            return res.status(400).json({ error: 'The selected sheet or range is empty.' });
        }

        const rowCount = data.length;
        const columnCount = data[0].length;

        // 2. Create file record in Supabase
        const sourceInfo = {
            type: 'google_sheet',
            spreadsheetId,
            sheetName,
            range: range || 'A1:Z5000',
            refreshMode: 'manual'
        };

        const fileId = await supabaseService.createFile(
            parseInt(userId),
            title || `GS: ${sheetName}`,
            'application/vnd.google-apps.spreadsheet',
            0, // Size not easily available
            1, // We import one sheet at a time for now
            sourceInfo
        );

        // 3. Create sheet record
        const sheetId = await supabaseService.createSheet(
            fileId,
            sheetName,
            0,
            rowCount,
            columnCount
        );

        // 4. Insert row data in batches
        const batchSize = 100;
        for (let rowIndex = 0; rowIndex < data.length; rowIndex++) {
            await supabaseService.createExcelData(sheetId, rowIndex, data[rowIndex]);
            if ((rowIndex + 1) % batchSize === 0) {
                console.log(`  Inserted ${rowIndex + 1}/${data.length} rows for Google Sheet`);
            }
        }

        res.json({
            message: 'Google Sheet imported successfully',
            fileId,
            sheetName,
            rowCount,
            data
        });
    } catch (error) {
        console.error('Google Sheets import error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/google-sheets/refresh/:fileId', async (req, res) => {
    try {
        const fileId = parseInt(req.params.fileId);
        const file = await supabaseService.getFileById(fileId);

        if (!file || !file.source_info || file.source_info.type !== 'google_sheet') {
            return res.status(400).json({ error: 'File is not a Google Sheet or missing source info' });
        }

        const { spreadsheetId, sheetName, range } = file.source_info;
        console.log(`Refreshing Google Sheet: ${spreadsheetId}, Sheet: ${sheetName}`);

        // 1. Fetch fresh data
        const data = await googleSheetsService.getSheetData(spreadsheetId, sheetName, range);
        if (!data || data.length === 0) {
            return res.status(400).json({ error: 'The Google Sheet is now empty.' });
        }

        // 2. Update data in Supabase
        await supabaseService.updateFileData(fileId, [{ name: sheetName, data }]);

        res.json({
            message: 'Google Sheet refreshed successfully',
            rowCount: data.length,
            updatedAt: new Date().toISOString(),
            data
        });
    } catch (error) {
        console.error('Google Sheets refresh error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// ============================================
// SharePoint OAuth Endpoints (Per-User Authentication)
// ============================================

/**
 * Initiates SharePoint OAuth flow - redirects user to Microsoft login
 */
app.get('/auth/sharepoint/start', async (req, res) => {
    try {
        const { userId } = req.query;

        if (!userId) {
            return res.status(400).json({ error: 'Missing userId parameter' });
        }

        if (!sharepointOAuthService.isConfigured()) {
            return res.status(503).json({
                error: 'SharePoint OAuth is not properly configured. Please contact your administrator.'
            });
        }

        // Generate authorization URL
        const authUrl = sharepointOAuthService.getAuthorizationUrl(userId);

        console.log(`Redirecting user ${userId} to SharePoint OAuth: ${authUrl}`);

        // Redirect user to Microsoft login
        res.redirect(authUrl);

    } catch (error) {
        console.error('SharePoint OAuth start error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

/**
 * SharePoint OAuth callback - exchanges code for tokens
 */
app.get('/auth/sharepoint/callback', async (req, res) => {
    try {
        const { code, state, error, error_description } = req.query;

        // Handle OAuth errors
        if (error) {
            console.error('SharePoint OAuth error:', error, error_description);
            const frontendUrl = process.env.FRONTEND_URL || 'http://localhost:5173';
            return res.redirect(`${frontendUrl}?sharepoint_error=${encodeURIComponent(error_description || error)}`);
        }

        if (!code || !state) {
            return res.status(400).json({ error: 'Missing authorization code or state' });
        }

        // Decode state to get userId
        const stateData = JSON.parse(Buffer.from(state, 'base64').toString('utf8'));
        const { userId } = stateData;

        if (!userId) {
            return res.status(400).json({ error: 'Invalid state parameter' });
        }

        console.log(`Processing SharePoint OAuth callback for user ${userId}`);

        // Exchange code for tokens
        const { accessToken, refreshToken, expiresIn } = await sharepointOAuthService.exchangeCodeForTokens(code);

        // Store tokens in database (encrypted)
        await sharepointOAuthService.storeUserTokens(userId, accessToken, refreshToken, expiresIn);

        console.log(`SharePoint connection established for user ${userId}`);

        // Redirect back to frontend with success
        const frontendUrl = process.env.FRONTEND_URL || 'http://localhost:5173';
        res.redirect(`${frontendUrl}?sharepoint_connected=true`);

    } catch (error) {
        console.error('SharePoint OAuth callback error:', error.message);
        const frontendUrl = process.env.FRONTEND_URL || 'http://localhost:5173';
        res.redirect(`${frontendUrl}?sharepoint_error=${encodeURIComponent(error.message)}`);
    }
});

/**
 * Check if user has connected their SharePoint account
 */
app.get('/api/sharepoint/connection-status', async (req, res) => {
    try {
        const { userId } = req.query;

        if (!userId) {
            return res.status(400).json({ error: 'Missing userId parameter' });
        }

        const isConnected = await sharepointOAuthService.isUserConnected(parseInt(userId));

        res.json({
            connected: isConnected,
            oauthConfigured: sharepointOAuthService.isConfigured()
        });

    } catch (error) {
        console.error('SharePoint connection status error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

/**
 * Disconnect user's SharePoint account (revoke tokens)
 */
app.delete('/api/sharepoint/disconnect', async (req, res) => {
    try {
        const { userId } = req.body;

        if (!userId) {
            return res.status(400).json({ error: 'Missing userId parameter' });
        }

        await sharepointOAuthService.disconnectUser(parseInt(userId));

        res.json({ message: 'SharePoint account disconnected successfully' });

    } catch (error) {
        console.error('SharePoint disconnect error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

/**
 * Get SharePoint sites for authenticated user
 */
app.post('/api/sharepoint/user/sites', async (req, res) => {
    try {
        const { userId } = req.body;

        if (!userId) {
            return res.status(400).json({ error: 'Missing userId parameter' });
        }

        // Get user's access token (will refresh if needed)
        const accessToken = await sharepointOAuthService.getUserAccessToken(parseInt(userId));

        // Fetch sites using user's token
        const sites = await sharepointService.getUserSites(accessToken);

        res.json({ sites });

    } catch (error) {
        console.error('SharePoint user sites error:', error.message);

        // If user not connected, return specific error
        if (error.message.includes('not connected')) {
            return res.status(401).json({
                error: 'Please connect your SharePoint account first',
                requiresAuth: true
            });
        }

        res.status(500).json({ error: error.message });
    }
});

/**
 * Get SharePoint lists for authenticated user
 */
app.post('/api/sharepoint/user/lists', async (req, res) => {
    try {
        const { userId, siteId } = req.body;

        if (!userId || !siteId) {
            return res.status(400).json({ error: 'Missing userId or siteId parameter' });
        }

        // Get user's access token
        const accessToken = await sharepointOAuthService.getUserAccessToken(parseInt(userId));

        // Fetch lists using user's token
        const lists = await sharepointService.getUserLists(accessToken, siteId);

        res.json({ lists });

    } catch (error) {
        console.error('SharePoint user lists error:', error.message);

        if (error.message.includes('not connected')) {
            return res.status(401).json({
                error: 'Please connect your SharePoint account first',
                requiresAuth: true
            });
        }

        res.status(500).json({ error: error.message });
    }
});

/**
 * Import SharePoint list using user's OAuth token
 */
app.post('/api/sharepoint/user/import', async (req, res) => {
    try {
        const { userId, siteId, listId, listName, siteName, title } = req.body;

        if (!userId || !siteId || !listId) {
            return res.status(400).json({ error: 'Missing required parameters' });
        }

        console.log(`Importing SharePoint list for user ${userId}: ${listId} from site: ${siteId}`);

        // Get user's access token
        const accessToken = await sharepointOAuthService.getUserAccessToken(parseInt(userId));

        // Fetch data using user's token
        const result = await sharepointService.importUserList(accessToken, siteId, listId);

        if (!result.data || result.data.length === 0) {
            return res.status(400).json({ error: 'The selected list is empty.' });
        }

        const { data, columns } = result;
        const rowCount = data.length;
        const columnCount = columns.length;

        // Create file record in Supabase
        const sourceInfo = {
            type: 'sharepoint_oauth',
            siteId,
            siteName: siteName || 'SharePoint Site',
            listId,
            listName: listName || 'SharePoint List',
            refreshMode: 'manual',
            userId: parseInt(userId)  // Track which user imported this
        };

        const fileId = await supabaseService.createFile(
            parseInt(userId),
            title || `SP: ${listName}`,
            'application/vnd.ms-sharepoint',
            0,
            1,
            sourceInfo
        );

        // Create sheet record
        const sheetId = await supabaseService.createSheet(
            fileId,
            listName || 'SharePoint List',
            0,
            rowCount,
            columnCount
        );

        // Insert row data in batches
        const batchSize = 100;
        for (let rowIndex = 0; rowIndex < data.length; rowIndex++) {
            await supabaseService.createExcelData(sheetId, rowIndex, data[rowIndex]);
            if ((rowIndex + 1) % batchSize === 0) {
                console.log(`  Inserted ${rowIndex + 1}/${data.length} rows for SharePoint list`);
            }
        }

        console.log('SharePoint OAuth import completed successfully');

        res.json({
            message: 'SharePoint list imported successfully',
            fileId,
            listName,
            rowCount,
            data
        });

    } catch (error) {
        console.error('SharePoint user import error:', error.message);

        if (error.message.includes('not connected')) {
            return res.status(401).json({
                error: 'Please connect your SharePoint account first',
                requiresAuth: true
            });
        }

        res.status(500).json({ error: error.message });
    }
});

// ============================================
// SharePoint Endpoints (Legacy - Service Account)
// These are kept for backward compatibility
// ============================================

app.get('/api/sharepoint/config-status', async (req, res) => {
    try {
        const isConfigured = sharepointService.isConfigured();
        res.json({ configured: isConfigured });
    } catch (error) {
        console.error('SharePoint config check error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/sharepoint/sites', async (req, res) => {
    try {
        if (!sharepointService.isConfigured()) {
            return res.status(503).json({
                error: 'SharePoint is not configured. Please contact your administrator to set up SharePoint integration.'
            });
        }

        const sites = await sharepointService.getSites();
        res.json({ sites });
    } catch (error) {
        console.error('SharePoint sites error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/sharepoint/lists', async (req, res) => {
    try {
        const { siteId } = req.body;
        if (!siteId) return res.status(400).json({ error: 'Missing siteId' });

        const lists = await sharepointService.getLists(siteId);
        res.json({ lists });
    } catch (error) {
        console.error('SharePoint lists error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/sharepoint/metadata', async (req, res) => {
    try {
        const { siteId, listId } = req.body;
        if (!siteId || !listId) {
            return res.status(400).json({ error: 'Missing siteId or listId' });
        }

        const columns = await sharepointService.getListColumns(siteId, listId);
        res.json({ siteId, listId, columns });
    } catch (error) {
        console.error('SharePoint metadata error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/sharepoint/import', async (req, res) => {
    try {
        const { userId, siteId, listId, listName, siteName, title } = req.body;

        if (!userId || !siteId || !listId) {
            return res.status(400).json({ error: 'Missing required parameters' });
        }

        console.log(`Importing SharePoint list: ${listId} from site: ${siteId}`);

        // 1. Fetch data from SharePoint
        const result = await sharepointService.importList(siteId, listId);

        if (!result.data || result.data.length === 0) {
            return res.status(400).json({ error: 'The selected list is empty.' });
        }

        const { data, columns } = result;
        const rowCount = data.length;
        const columnCount = columns.length;

        // 2. Create file record in Supabase
        const sourceInfo = {
            type: 'sharepoint',
            siteId,
            siteName: siteName || 'SharePoint Site',
            listId,
            listName: listName || 'SharePoint List',
            refreshMode: 'manual'
        };

        const fileId = await supabaseService.createFile(
            parseInt(userId),
            title || `SP: ${listName}`,
            'application/vnd.ms-sharepoint',
            0, // Size not easily available
            1, // One list at a time
            sourceInfo
        );

        // 3. Create sheet record
        const sheetId = await supabaseService.createSheet(
            fileId,
            listName || 'SharePoint List',
            0,
            rowCount,
            columnCount
        );

        // 4. Insert row data in batches
        const batchSize = 100;
        for (let rowIndex = 0; rowIndex < data.length; rowIndex++) {
            await supabaseService.createExcelData(sheetId, rowIndex, data[rowIndex]);
            if ((rowIndex + 1) % batchSize === 0) {
                console.log(`  Inserted ${rowIndex + 1}/${data.length} rows for SharePoint list`);
            }
        }

        console.log('SharePoint import completed successfully');

        res.json({
            message: 'SharePoint list imported successfully',
            fileId,
            listName,
            rowCount,
            data
        });
    } catch (error) {
        console.error('SharePoint import error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/sharepoint/refresh/:fileId', async (req, res) => {
    try {
        const fileId = parseInt(req.params.fileId);
        const file = await supabaseService.getFileById(fileId);

        if (!file || !file.source_info || file.source_info.type !== 'sharepoint') {
            return res.status(400).json({ error: 'File is not a SharePoint list or missing source info' });
        }

        const { siteId, listId } = file.source_info;
        console.log(`Refreshing SharePoint list: ${listId} from site: ${siteId}`);

        // 1. Fetch fresh data
        const result = await sharepointService.importList(siteId, listId);

        if (!result.data || result.data.length === 0) {
            return res.status(400).json({ error: 'The SharePoint list is now empty.' });
        }

        // 2. Update data in Supabase
        const listName = file.source_info.listName || 'SharePoint List';
        await supabaseService.updateFileData(fileId, [{ name: listName, data: result.data }]);

        res.json({
            message: 'SharePoint list refreshed successfully',
            rowCount: result.data.length,
            updatedAt: new Date().toISOString(),
            data: result.data
        });
    } catch (error) {
        console.error('SharePoint refresh error:', error.message);
        res.status(500).json({ error: error.message });
    }
});


// ============================================
// SQL Database Endpoints (MySQL & PostgreSQL)
// ============================================

/**
 * Test SQL database connection
 */
app.post('/api/sql/test-connection', async (req, res) => {
    try {
        const { host, port, user, password, database, type } = req.body;

        if (!host || !user || !database || !type) {
            return res.status(400).json({ error: 'Missing required parameters: host, user, database, type' });
        }

        console.log(`Testing ${type} connection to ${host}:${port || 'default'}, database: ${database}`);

        const result = await sqlDatabaseService.testConnection({
            host,
            port: port ? parseInt(port) : undefined,
            user,
            password: password || '',
            database,
            type
        });

        res.json({
            success: true,
            message: result.message,
            type
        });

    } catch (error) {
        console.error('SQL connection test error:', error.message);
        res.status(500).json({
            success: false,
            error: error.message
        });
    }
});

/**
 * Get list of tables from SQL database
 */
app.post('/api/sql/tables', async (req, res) => {
    try {
        const { host, port, user, password, database, type } = req.body;

        if (!host || !user || !database || !type) {
            return res.status(400).json({ error: 'Missing required parameters' });
        }

        console.log(`Fetching tables from ${type} database: ${database}`);

        const tables = await sqlDatabaseService.getTables({
            host,
            port: port ? parseInt(port) : undefined,
            user,
            password: password || '',
            database,
            type
        });

        res.json({ tables });

    } catch (error) {
        console.error('SQL get tables error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

/**
 * Import table data from SQL database
 */
app.post('/api/sql/import', async (req, res) => {
    try {
        const { userId, host, port, user, password, database, type, tableName, title } = req.body;

        if (!userId || !host || !user || !database || !type || !tableName) {
            return res.status(400).json({ error: 'Missing required parameters' });
        }

        console.log(`Importing table "${tableName}" from ${type} database: ${database}`);

        // 1. Fetch data from SQL database
        const result = await sqlDatabaseService.importTable({
            host,
            port: port ? parseInt(port) : undefined,
            user,
            password: password || '',
            database,
            type
        }, tableName);

        if (!result.data || result.data.length === 0) {
            return res.status(400).json({ error: 'The selected table is empty.' });
        }

        const { data, columns } = result;
        const rowCount = data.length;
        const columnCount = columns.length;

        // 2. Create file record in Supabase
        const sourceInfo = {
            type: `sql_${type}`,
            host,
            port: port || (type === 'postgresql' ? 5432 : 3306),
            database,
            tableName,
            refreshMode: 'manual'
        };

        const fileId = await supabaseService.createFile(
            parseInt(userId),
            title || `${type.toUpperCase()}: ${tableName}`,
            `application/x-${type}`,
            0,
            1,
            sourceInfo
        );

        // 3. Create sheet record
        const sheetId = await supabaseService.createSheet(
            fileId,
            tableName,
            0,
            rowCount - 1, // Subtract header row
            columnCount
        );

        // 4. Insert row data in batches  
        const batchSize = 100;
        for (let rowIndex = 0; rowIndex < data.length; rowIndex++) {
            await supabaseService.createExcelData(sheetId, rowIndex, data[rowIndex]);
            if ((rowIndex + 1) % batchSize === 0) {
                console.log(`  Inserted ${rowIndex + 1}/${data.length} rows for SQL table`);
            }
        }

        console.log('SQL import completed successfully');

        res.json({
            message: 'SQL table imported successfully',
            fileId,
            tableName,
            rowCount: rowCount - 1, // Actual data rows (excluding header)
            data
        });

    } catch (error) {
        console.error('SQL import error:', error.message);
        res.status(500).json({ error: error.message });
    }
});


app.listen(port, () => {
    console.log(`Server running on port ${port} with Supabase integration`);
    console.log('Supabase Configuration:');
    console.log(`  Project URL: ${supabaseService.supabaseUrl}`);
});



