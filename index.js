const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '.env') });

const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const fs = require('fs');
const XLSX = require('xlsx');
const supabaseService = require('./supabaseService');
const brevoService = require('./brevoService');
const googleSheetsService = require('./googleSheetsService');
const sharepointService = require('./sharepointService');
const sharepointOAuthService = require('./sharepointOAuthService');
const dbConnectorService = require('./dbConnectorService');
const sqlParserService = require('./sqlParserService');

const app = express();
const port = process.env.PORT || 3001;

// CORS Configuration - Allow specific origins
const allowedOrigins = [
    'http://localhost:3000',
    'http://localhost:3001',
    'http://localhost:3002',  // Current frontend port
    'http://localhost:5173',  // Vite dev server
    'http://192.168.0.101:3000',  // Added network access URL
    'http://192.168.0.102:3000',  // Network access URL
    'http://192.168.1.5:3000',  // Current network access URL
    'http://192.168.1.5:3001',  // Backend network URL
    'https://analyticcore-server.onrender.com',
    'http://139.59.32.39',  // Digital Ocean Droplet
    'http://139.59.32.39:3001',  // Digital Ocean Droplet with port
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
 
// Global Request Logger Middleware
app.use((req, res, next) => {
    console.log(`📡 [Incoming Request] ${req.method} ${req.url}`);
    next();
});

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

app.get('/api/health', (req, res) => {
    res.json({
        status: 'ok',
        version: '1.0.7-folder-fetch-fix',
        uptime: process.uptime(),
        timestamp: new Date().toISOString()
    });
});

// Helper: Generate a random temporary password
function generateTempPassword(length = 10) {
    const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZabcdefghjkmnpqrstuvwxyz23456789!@#$';
    let password = '';
    for (let i = 0; i < length; i++) {
        password += chars.charAt(Math.floor(Math.random() * chars.length));
    }
    return password;
}

// Helper: Generate a 6-digit OTP
function generateOtp() {
    return Math.floor(100000 + Math.random() * 900000).toString();
}

// Auth Endpoints
app.post('/api/login', async (req, res) => {
    try {
        const { email, password } = req.body;
        const user = await supabaseService.getUserByEmail(email);

        if (user && user.password === password) {
            // Don't send password back
            const { password, otp_code, otp_expires_at, ...userWithoutSensitive } = user;
            res.json(userWithoutSensitive);
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
        const { name, email, phone, company, job_title, domain } = req.body;

        // Check if user already exists
        const existingUser = await supabaseService.getUserByEmail(email);
        if (existingUser) {
            return res.status(400).json({ error: 'Email already exists' });
        }

        // Generate temporary password
        const tempPassword = generateTempPassword();

        const newUser = await supabaseService.createUser(
            name, email, tempPassword, 'USER',
            phone, company, job_title, domain,
            null, false, true // must_change_password = true
        );

        // Send temporary password email
        await brevoService.sendTemporaryPasswordEmail(email, name, tempPassword);

        res.json({ ...newUser, emailSent: true });
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

app.post('/api/admin/users/bulk', async (req, res) => {
    try {
        const { users } = req.body;
        if (!users || !Array.isArray(users)) {
            return res.status(400).json({ error: 'Invalid users data' });
        }

        const results = {
            total: users.length,
            success: 0,
            failed: 0,
            errors: []
        };

        for (const userData of users) {
            try {
                const { name, email, role, phone, company, job_title, domain } = userData;
                
                // Check if user already exists
                const existingUser = await supabaseService.getUserByEmail(email);
                if (existingUser) {
                    results.failed++;
                    results.errors.push({ email, error: 'Email already exists' });
                    continue;
                }

                // Generate temporary password for each user
                const tempPassword = generateTempPassword();

                await supabaseService.createUser(
                    name, 
                    email, 
                    tempPassword,
                    role || 'USER', 
                    phone, 
                    company, 
                    job_title, 
                    domain,
                    null, false, true // must_change_password = true
                );

                // Send temporary password email (non-blocking)
                brevoService.sendTemporaryPasswordEmail(email, name, tempPassword)
                    .catch(err => console.error(`Failed to send email to ${email}:`, err.message));

                results.success++;
            } catch (err) {
                results.failed++;
                results.errors.push({ email: userData.email, error: err.message });
            }
        }

        res.json(results);
    } catch (error) {
        console.error('Bulk user creation error:', error);
        res.status(500).json({ error: error.message });
    }
});

// ============================================
// Password Management & Forgot Password
// ============================================

// Change password (for logged-in users / first-time password change)
app.put('/api/users/:id/password', async (req, res) => {
    try {
        const userId = parseInt(req.params.id);
        const { currentPassword, newPassword } = req.body;

        if (!newPassword || newPassword.length < 8) {
            return res.status(400).json({ error: 'New password must be at least 8 characters' });
        }

        // Verify current password
        const { data: user, error: fetchError } = await supabaseService.supabase
            .from('users')
            .select('password')
            .eq('id', userId)
            .single();

        if (fetchError || !user) {
            return res.status(404).json({ error: 'User not found' });
        }

        if (user.password !== currentPassword) {
            return res.status(401).json({ error: 'Current password is incorrect' });
        }

        await supabaseService.updateUserPassword(userId, newPassword);
        res.json({ message: 'Password updated successfully' });
    } catch (error) {
        console.error('Change password error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Request forgot password OTP
app.post('/api/auth/forgot-password', async (req, res) => {
    try {
        const { email } = req.body;
        if (!email) return res.status(400).json({ error: 'Email is required' });

        const user = await supabaseService.getUserByEmail(email);
        if (!user) {
            // Don't reveal if email exists or not for security
            return res.json({ message: 'If the email exists, an OTP has been sent' });
        }

        const otp = generateOtp();
        const expiresAt = new Date(Date.now() + 10 * 60 * 1000).toISOString(); // 10 minutes

        await supabaseService.setUserOtp(email, otp, expiresAt);
        await brevoService.sendOtpEmail(email, user.name, otp);

        res.json({ message: 'OTP sent to your email' });
    } catch (error) {
        console.error('Forgot password error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Verify OTP
app.post('/api/auth/verify-otp', async (req, res) => {
    try {
        const { email, otp } = req.body;
        if (!email || !otp) return res.status(400).json({ error: 'Email and OTP are required' });

        const user = await supabaseService.getUserByEmail(email);
        if (!user) {
            return res.status(400).json({ error: 'Invalid email or OTP' });
        }

        if (!user.otp_code || user.otp_code !== otp) {
            return res.status(400).json({ error: 'Invalid OTP' });
        }

        if (new Date(user.otp_expires_at) < new Date()) {
            await supabaseService.clearUserOtp(email);
            return res.status(400).json({ error: 'OTP has expired. Please request a new one.' });
        }

        res.json({ message: 'OTP verified successfully', verified: true });
    } catch (error) {
        console.error('Verify OTP error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Reset password with OTP
app.post('/api/auth/reset-password', async (req, res) => {
    try {
        const { email, otp, newPassword } = req.body;
        if (!email || !otp || !newPassword) {
            return res.status(400).json({ error: 'Email, OTP, and new password are required' });
        }

        if (newPassword.length < 8) {
            return res.status(400).json({ error: 'Password must be at least 8 characters' });
        }

        const user = await supabaseService.getUserByEmail(email);
        if (!user || !user.otp_code || user.otp_code !== otp) {
            return res.status(400).json({ error: 'Invalid email or OTP' });
        }

        if (new Date(user.otp_expires_at) < new Date()) {
            await supabaseService.clearUserOtp(email);
            return res.status(400).json({ error: 'OTP has expired' });
        }

        await supabaseService.updateUserPasswordByEmail(email, newPassword);
        res.json({ message: 'Password reset successfully' });
    } catch (error) {
        console.error('Reset password error:', error);
        res.status(500).json({ error: error.message });
    }
});

// ============================================
// Organization Endpoints
// ============================================

app.post('/api/organizations', async (req, res) => {
    try {
        const { name } = req.body;
        if (!name) return res.status(400).json({ error: 'Missing organization name' });
        const org = await supabaseService.createOrganization(name);
        res.json(org);
    } catch (error) {
        console.error('Create organization error:', error);
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/organizations', async (req, res) => {
    try {
        const orgs = await supabaseService.getOrganizations();
        res.json(orgs);
    } catch (error) {
        console.error('Get organizations error:', error);
        res.status(500).json({ error: error.message });
    }
});

app.delete('/api/organizations/:id', async (req, res) => {
    try {
        await supabaseService.deleteOrganization(req.params.id);
        res.json({ message: 'Organization deleted' });
    } catch (error) {
        console.error('Delete organization error:', error);
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/organizations/:id/users', async (req, res) => {
    try {
        const users = await supabaseService.getUsersByOrganization(req.params.id);
        res.json(users);
    } catch (error) {
        console.error('Get organization users error:', error);
        res.status(500).json({ error: error.message });
    }
});

// ============================================
// User Organization & Superuser Endpoints
// ============================================

app.put('/api/users/:id/organization', async (req, res) => {
    try {
        const userId = parseInt(req.params.id);
        const { organizationId } = req.body;
        await supabaseService.updateUserOrganization(userId, organizationId);
        res.json({ message: 'User organization updated' });
    } catch (error) {
        console.error('Update user organization error:', error);
        res.status(500).json({ error: error.message });
    }
});

app.put('/api/users/:id/superuser', async (req, res) => {
    try {
        const userId = parseInt(req.params.id);
        const { isSuperuser } = req.body;
        await supabaseService.updateUserSuperuser(userId, isSuperuser);
        res.json({ message: 'User superuser status updated' });
    } catch (error) {
        console.error('Update user superuser error:', error);
        res.status(500).json({ error: error.message });
    }
});

// ============================================
// Dashboard Access Sharing Endpoints
// ============================================

app.post('/api/dashboards/:id/access', async (req, res) => {
    try {
        const dashboardId = parseInt(req.params.id);
        const { userId, accessLevel, grantedBy } = req.body;
        if (!userId || !accessLevel || !grantedBy) {
            return res.status(400).json({ error: 'Missing userId, accessLevel, or grantedBy' });
        }
        const result = await supabaseService.grantDashboardAccess(dashboardId, userId, accessLevel, grantedBy);
        res.json(result);
    } catch (error) {
        console.error('Grant dashboard access error:', error);
        const status = error.message.includes('owner') || error.message.includes('co-owner') ? 403 : 500;
        res.status(status).json({ error: error.message });
    }
});

app.delete('/api/dashboards/:id/access/:userId', async (req, res) => {
    try {
        const dashboardId = parseInt(req.params.id);
        const userId = parseInt(req.params.userId);
        const requestingUserId = parseInt(req.query.requestingUserId);
        if (!requestingUserId) return res.status(400).json({ error: 'Missing requestingUserId' });
        await supabaseService.revokeDashboardAccess(dashboardId, userId, requestingUserId);
        res.json({ message: 'Access revoked' });
    } catch (error) {
        console.error('Revoke dashboard access error:', error);
        const status = error.message.includes('owner') || error.message.includes('co-owner') ? 403 : 500;
        res.status(status).json({ error: error.message });
    }
});

app.get('/api/dashboards/:id/access', async (req, res) => {
    try {
        const dashboardId = parseInt(req.params.id);
        const accessList = await supabaseService.getDashboardAccessList(dashboardId);
        res.json(accessList);
    } catch (error) {
        console.error('Get dashboard access error:', error);
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/dashboards/shared', async (req, res) => {
    try {
        const userId = parseInt(req.query.userId);
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        const dashboards = await supabaseService.getSharedDashboards(userId);
        res.json(dashboards);
    } catch (error) {
        console.error('Get shared dashboards error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Dashboard Endpoints
app.post('/api/dashboards', async (req, res) => {
    try {
        const { userId, dashboard } = req.body;

        console.log('💾 Received save dashboard request:', {
            userId,
            dashboardName: dashboard?.name,
            hasDataModel: !!dashboard?.dataModel,
            chartConfigsCount: dashboard?.chartConfigs?.length,
            sectionsCount: dashboard?.sections?.length,
            filterColumnsCount: dashboard?.filterColumns?.length,
            folderId: dashboard?.folderId,
            isWorkspace: dashboard?.isWorkspace
        });

        if (!dashboard) {
            console.error('❌ Missing dashboard data in request body');
            return res.status(400).json({ error: 'Missing dashboard data' });
        }

        if (!userId) {
            console.error('❌ Missing userId in request body');
            return res.status(400).json({ error: 'Missing userId' });
        }

        const { name, dataModel, chartConfigs, sections, filterColumns, folderId, isWorkspace } = dashboard;
        
        if (!name || !dataModel || !chartConfigs) {
            console.error('❌ Missing required dashboard fields:', { hasName: !!name, hasDataModel: !!dataModel, hasChartConfigs: !!chartConfigs });
            return res.status(400).json({ error: 'Missing required dashboard fields (name, dataModel, or chartConfigs)' });
        }

        console.log('✅ Validation passed, calling createDashboard...');
        const result = await supabaseService.createDashboard(userId, name, dataModel, chartConfigs, sections, filterColumns, folderId, isWorkspace);
        console.log('✅ Dashboard created successfully:', result);
        res.json(result);
    } catch (error) {
        console.error('❌ Save dashboard error:', {
            message: error.message,
            stack: error.stack,
            name: error.name,
            code: error.code
        });
        res.status(500).json({ 
            error: error.message || 'Unknown error occurred',
            details: error.code || error.name
        });
    }
});

app.put('/api/dashboards/:id', async (req, res) => {
    try {
        const { id: rawId } = req.params;
        const id = parseInt(rawId);
        const { dashboard } = req.body;

        console.log('🔄 Attemping to update dashboard:', { id, rawId, name: dashboard?.name });

        if (!dashboard) {
            console.error('❌ Update failed: Missing dashboard data');
            return res.status(400).json({ error: 'Missing dashboard data' });
        }

        const { name, dataModel, chartConfigs, sections, filterColumns, folderId, isWorkspace } = dashboard;
        const result = await supabaseService.updateDashboard(id, name, dataModel, chartConfigs, sections, filterColumns, folderId, isWorkspace);
        console.log('✅ Dashboard updated successfully:', id);
        res.json(result);
    } catch (error) {
        console.error('❌ Update dashboard error:', error);
        res.status(500).json({ error: error.message || 'Unknown server error during update' });
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

// Get a single dashboard by id (used for live polling of refreshed data)
app.get('/api/dashboards/:id', async (req, res) => {
    try {
        const id = parseInt(req.params.id);
        if (!id || isNaN(id)) return res.status(400).json({ error: 'Invalid dashboard id' });
        const dashboard = await supabaseService.getDashboardById(id);
        if (!dashboard) return res.status(404).json({ error: 'Dashboard not found' });
        res.json(dashboard);
    } catch (error) {
        console.error('Get dashboard by id error:', error);
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

// ============================================
// Workspace Folder Endpoints
// ============================================

// Create a workspace folder
app.post('/api/workspace/folders', async (req, res) => {
    try {
        const { ownerId, name, accessUsers = [], accessGroups = [] } = req.body;
        if (!ownerId || !name) {
            return res.status(400).json({ error: 'Missing ownerId or name' });
        }
        const folder = await supabaseService.createWorkspaceFolder(ownerId, name, accessUsers, accessGroups);
        res.json(folder);
    } catch (error) {
        console.error('Create workspace folder error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Get all folders accessible to a user
app.get('/api/workspace/folders', async (req, res) => {
    try {
        const userId = parseInt(req.query.userId);
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        const folders = await supabaseService.getAccessibleFolders(userId);
        res.json(folders);
    } catch (error) {
        console.error('Get workspace folders error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Update a workspace folder (owner only)
app.put('/api/workspace/folders/:id', async (req, res) => {
    try {
        const folderId = req.params.id;
        const { name, accessUsers = [], accessGroups = [], requestingUserId } = req.body;
        if (!name || !requestingUserId) {
            return res.status(400).json({ error: 'Missing name or requestingUserId' });
        }
        await supabaseService.updateWorkspaceFolder(folderId, name, accessUsers, accessGroups, requestingUserId);
        res.json({ message: 'Folder updated' });
    } catch (error) {
        console.error('Update workspace folder error:', error);
        const status = error.message.includes('owner') ? 403 : 500;
        res.status(status).json({ error: error.message });
    }
});

// Delete a workspace folder (owner only)
app.delete('/api/workspace/folders/:id', async (req, res) => {
    try {
        const folderId = req.params.id;
        const requestingUserId = parseInt(req.query.userId);
        if (!requestingUserId) return res.status(400).json({ error: 'Missing userId' });
        await supabaseService.deleteWorkspaceFolder(folderId, requestingUserId);
        res.json({ message: 'Folder deleted' });
    } catch (error) {
        console.error('Delete workspace folder error:', error);
        const status = error.message.includes('owner') ? 403 : 500;
        res.status(status).json({ error: error.message });
    }
});

// Get dashboards in a folder (access checked)
app.get('/api/workspace/folders/:id/dashboards', async (req, res) => {
    try {
        const folderId = req.params.id;
        const userId = parseInt(req.query.userId);
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        const result = await supabaseService.getFolderDashboards(folderId, userId);
        res.json(result);
    } catch (error) {
        console.error('Get folder dashboards error:', error);
        const status = error.message.toLowerCase().includes('access denied') ? 403 : 500;
        res.status(status).json({ error: error.message });
    }
});

// ============================================
// Workspace Group Endpoints
// ============================================

// Create a workspace group
app.post('/api/workspace/groups', async (req, res) => {
    try {
        const { ownerId, name, userIds = [] } = req.body;
        if (!ownerId || !name) {
            return res.status(400).json({ error: 'Missing ownerId or name' });
        }
        const group = await supabaseService.createWorkspaceGroup(ownerId, name, userIds);
        res.json(group);
    } catch (error) {
        console.error('Create workspace group error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Get all groups created by a user
app.get('/api/workspace/groups', async (req, res) => {
    try {
        const userId = parseInt(req.query.userId);
        if (!userId) return res.status(400).json({ error: 'Missing userId' });
        const groups = await supabaseService.getWorkspaceGroups(userId);
        res.json(groups);
    } catch (error) {
        console.error('Get workspace groups error:', error);
        res.status(500).json({ error: error.message });
    }
});

// Update a workspace group
app.put('/api/workspace/groups/:id', async (req, res) => {
    try {
        const groupId = req.params.id;
        const { name, userIds = [], requestingUserId } = req.body;
        if (!name || !requestingUserId) {
            return res.status(400).json({ error: 'Missing name or requestingUserId' });
        }
        await supabaseService.updateWorkspaceGroup(groupId, name, userIds, requestingUserId);
        res.json({ message: 'Group updated' });
    } catch (error) {
        console.error('Update workspace group error:', error);
        const status = error.message.includes('owner') ? 403 : 500;
        res.status(status).json({ error: error.message });
    }
});

// Delete a workspace group
app.delete('/api/workspace/groups/:id', async (req, res) => {
    try {
        const groupId = req.params.id;
        const requestingUserId = parseInt(req.query.userId);
        if (!requestingUserId) return res.status(400).json({ error: 'Missing userId' });
        await supabaseService.deleteWorkspaceGroup(groupId, requestingUserId);
        res.json({ message: 'Group deleted' });
    } catch (error) {
        console.error('Delete workspace group error:', error);
        const status = error.message.includes('owner') ? 403 : 500;
        res.status(status).json({ error: error.message });
    }
});


app.post('/api/upload', upload.single('file'), async (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'No file uploaded' });
    }

    const { userId } = req.body;
    const { filename, originalname, path: filePath, mimetype, size } = req.file;

    try {
        // Check if it's an Excel or SQL file
        const extension = originalname.split('.').pop().toLowerCase();
        const isExcelFile = mimetype.includes('spreadsheet') || mimetype.includes('excel') ||
            extension === 'xlsx' || extension === 'xls';
        const isSqlFile = mimetype.includes('sql') || mimetype === 'text/plain' || extension === 'sql';

        if (!isExcelFile && !isSqlFile) {
            return res.status(400).json({ error: 'Only Excel (.xlsx, .xls) and SQL (.sql) dump files are supported' });
        }

        // Handle SQL files differently - just store file info and return
        if (isSqlFile) {
            const fileId = await supabaseService.createFile(
                parseInt(userId),
                originalname,
                'application/sql',
                size,
                0, // Sheet count not applicable for SQL
                { type: 'sql_dump', uploaded: true }
            );

            console.log(`SQL file uploaded with ID: ${fileId}`);

            return res.json({
                message: 'SQL file uploaded successfully',
                file: {
                    id: fileId,
                    originalName: originalname,
                    type: 'sql',
                    path: filePath
                }
            });
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

            // Insert row data - process in larger batches for high performance
            const batchSize = 500;
            let currentBatch = [];

            for (let rowIndex = 0; rowIndex < sheetData.length; rowIndex++) {
                currentBatch.push({
                    rowIndex,
                    rowData: sheetData[rowIndex]
                });

                // When batch is full or it's the last row, insert
                if (currentBatch.length === batchSize || rowIndex === sheetData.length - 1) {
                    await supabaseService.createExcelDataBatch(sheetId, currentBatch);
                    console.log(`  Inserted ${rowIndex + 1}/${sheetData.length} rows for sheet "${sheetName}"`);
                    currentBatch = []; // Reset batch
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

// Get File Content (for Preview/Import)
app.get('/api/uploads/:id/content', async (req, res) => {
    try {
        const id = parseInt(req.params.id);
        const fileContent = await supabaseService.getFileContent(id);

        console.log(`Retrieved file content for ID ${id} with ${fileContent.sheets.length} sheets`);
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
        const { userId, spreadsheetId, sheetNames, range, title } = req.body;

        if (!userId || !spreadsheetId || !sheetNames || !Array.isArray(sheetNames)) {
            return res.status(400).json({ error: 'Missing required parameters or sheetNames is not an array' });
        }

        console.log(`Importing ${sheetNames.length} Google Sheets: ${spreadsheetId}`);

        const importedResults = [];

        // 1. Create file record in Supabase once
        const sourceInfo = {
            type: 'google_sheet',
            spreadsheetId,
            sheets: sheetNames,
            range: range || 'A1:Z5000',
            refreshMode: 'manual',
            lastRefresh: new Date().toISOString()
        };

        const fileId = await supabaseService.createFile(
            parseInt(userId),
            title || `GS: ${spreadsheetId}`,
            'application/vnd.google-apps.spreadsheet',
            0,
            sheetNames.length,
            sourceInfo
        );

        // 2. Process each sheet
        for (let i = 0; i < sheetNames.length; i++) {
            const sheetName = sheetNames[i];
            console.log(`  Processing sheet: ${sheetName} (${i + 1}/${sheetNames.length})`);

            const data = await googleSheetsService.getSheetData(spreadsheetId, sheetName, range);

            if (data && data.length > 0) {
                const rowCount = data.length;
                const columnCount = data[0].length;

                // Create sheet record
                const sheetId = await supabaseService.createSheet(
                    fileId,
                    sheetName,
                    i,
                    rowCount,
                    columnCount
                );

                // Insert row data
                for (let rowIndex = 0; rowIndex < data.length; rowIndex++) {
                    await supabaseService.createExcelData(sheetId, rowIndex, data[rowIndex]);
                }

                importedResults.push({
                    id: sheetId,
                    name: sheetName,
                    data: data,
                    fileId: fileId
                });
            }
        }

        if (importedResults.length === 0) {
            return res.status(400).json({ error: 'None of the selected sheets contained data.' });
        }

        res.json({
            message: `Successfully imported ${importedResults.length} sheets`,
            title: title || `GS: ${spreadsheetId}`,
            sheets: importedResults
        });
    } catch (error) {
        console.error('Google Sheets multi-import error:', error.message);
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

        const { spreadsheetId, sheetName, sheets, range } = file.source_info;
        
        const targetSheets = sheets || (sheetName ? [sheetName] : []);
        if (targetSheets.length === 0) {
            return res.status(400).json({ error: 'No sheets configured for this file' });
        }

        const updatedSheets = [];
        let firstSheetData = null;

        for (const sheet of targetSheets) {
            console.log(`[REFRESH] Fetching sheet: "${sheet}" from Spreadsheet: ${spreadsheetId}`);
            try {
                const data = await googleSheetsService.getSheetData(spreadsheetId, sheet, range);
                console.log(`[REFRESH] Successfully fetched "${sheet}": ${data ? data.length : 0} rows`);
                if (data && data.length > 0) {
                    updatedSheets.push({ name: sheet, data });
                    if (!firstSheetData) firstSheetData = data;
                } else {
                    console.warn(`[REFRESH] Sheet "${sheet}" returned empty data`);
                }
            } catch (sheetError) {
                console.error(`[REFRESH] Error fetching sheet "${sheet}":`, sheetError.message);
                // Continue with other sheets if one fails, or re-throw? 
                // Let's re-throw to be safe and notify user, since partial refresh might be misleading.
                throw sheetError;
            }
        }

        if (updatedSheets.length === 0) {
            return res.status(400).json({ error: 'The Google Sheets are now empty.' });
        }

        // 2. Update data in Supabase
        await supabaseService.updateFileData(fileId, updatedSheets);

        res.json({
            message: 'Google Sheet refreshed successfully',
            rowCount: firstSheetData ? firstSheetData.length : 0,
            updatedAt: new Date().toISOString(),
            sheets: updatedSheets,   // ← Full sheets array (matches import response shape)
            data: firstSheetData     // ← Keep for backward compatibility
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

// SQL Dump Endpoints
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

        console.log(`[SQL Route] Testing ${type} connection to ${host}:${port || 'default'}, database: ${database}`);

        const result = await dbConnectorService.testConnection({
            host,
            port: port ? parseInt(port) : undefined,
            user,
            password: password || '',
            database,
            engine: type // Normalize to 'engine' for DbConnectorService
        });

        if (result.success) {
            res.json({
                success: true,
                message: result.message,
                type
            });
        } else {
            res.status(400).json({
                success: false,
                error: result.message
            });
        }

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

        console.log(`[SQL Route] Fetching tables from ${type} database: ${database}`);

        const tables = await dbConnectorService.getTables({
            host,
            port: port ? parseInt(port) : undefined,
            user,
            password: password || '',
            database,
            engine: type
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
        const result = await dbConnectorService.getTableData({
            host,
            port: port ? parseInt(port) : undefined,
            user,
            password: password || '',
            database,
            engine: type
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

// SQL File Import (Batch)
app.post('/api/sql/import-file', async (req, res) => {
    try {
        const { userId, fileId, tables, title } = req.body;

        if (!userId || !fileId || !tables || !Array.isArray(tables) || tables.length === 0) {
            return res.status(400).json({ error: 'Missing required parameters (userId, fileId, tables array)' });
        }

        console.log(`Importing SQL tables: ${tables.join(', ')} for user ${userId}`);

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
        const importedTables = [];

        // Process each table
        for (const table of tables) {
            console.log(`  Processing table: ${table}`);
            // Extract table data using SQL parser service
            const result = await sqlParserService.extractTableData(filePath, table);
            const { headers, rows } = result;

            if (!rows || rows.length === 0) {
                console.warn(`    Table ${table} is empty, skipping.`);
                continue;
            }

            // Note: sqlParserService.extractTableData returns [headers, ...dataRows]
            const rowCount = rows.length - 1;
            const columnCount = headers.length;

            // Create a new file record for this specific table import
            const sourceInfo = {
                type: 'sql_dump',
                table: table,
                originalFileId: fileId,
                refreshMode: 'static'
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
                rowCount + 1, // Include header row in count
                columnCount
            );

            // Insert ALL rows (including header at index 0)
            // This aligns with Excel import where row 0 is usually headers
            for (let rowIndex = 0; rowIndex < rows.length; rowIndex++) {
                await supabaseService.createExcelData(sheetId, rowIndex, rows[rowIndex]);
            }

            importedTables.push({
                fileId: importedFileId,
                tableName: table,
                rowCount,
                data: rows // This already contains [headers, ...data]
            });
        }

        if (importedTables.length === 0) {
            return res.status(400).json({ error: 'None of the selected tables contained data.' });
        }

        console.log(`Successfully imported ${importedTables.length} SQL tables`);

        // Log the first table's data structure for debugging
        if (importedTables.length > 0) {
            const first = importedTables[0];
            console.log(`First table "${first.tableName}" sample row 0 (headers):`, first.data[0]);
            console.log(`First table "${first.tableName}" sample row 1 (values):`, first.data[1]);
        }

        res.json({
            message: `Successfully imported ${importedTables.length} tables`,
            importedFiles: importedTables,
            // Return first one as the primary result for backward compatibility
            fileId: importedTables[0].fileId,
            tableName: importedTables[0].tableName,
            rowCount: importedTables[0].rowCount,
            data: importedTables[0].data
        });
    } catch (error) {
        console.error('SQL import error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// SQL Database Connection Endpoints (Live Import)
app.post('/api/sql-db/test', async (req, res) => {
    try {
        const { engine, host, port, database, user, password } = req.body;

        if (!engine || !host || !database || !user) {
            return res.status(400).json({ error: 'Missing required connection parameters' });
        }

        console.log(`Testing ${engine} database connection to ${host}:${port}/${database}`);

        const result = await dbConnectorService.testConnection({
            engine,
            host,
            port,
            database,
            user,
            password
        });

        if (result.success) {
            res.json({ success: true, message: result.message });
        } else {
            res.status(400).json({ success: false, error: result.message });
        }
    } catch (error) {
        console.error('SQL database test error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/sql-db/tables', async (req, res) => {
    try {
        const { engine, host, port, database, user, password } = req.body;

        if (!engine || !host || !database || !user) {
            return res.status(400).json({ error: 'Missing required connection parameters' });
        }

        console.log(`Fetching tables from ${engine} database: ${host}:${port}/${database}`);

        const tables = await dbConnectorService.getTables({
            engine,
            host,
            port,
            database,
            user,
            password
        });

        res.json({ tables });
    } catch (error) {
        console.error('SQL database tables error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/sql-db/import', async (req, res) => {
    try {
        const { userId, engine, host, port, database, user, password, tableNames, title } = req.body;

        if (!userId || !engine || !host || !database || !user || !tableNames || !Array.isArray(tableNames)) {
            return res.status(400).json({ error: 'Missing required parameters or tableNames is not an array' });
        }

        console.log(`Importing ${tableNames.length} tables from ${engine} database: ${host}:${port}/${database}`);

        const importedResults = [];

        // 1. Create file record in Supabase once (represents the connection/import set)
        const sourceInfo = {
            type: 'sql_database',
            tables: tableNames, // Store all tables in metadata
            engine: engine,
            host: host,
            port: port,
            database: database,
            user: user,
            refreshMode: 'manual',
            lastRefresh: new Date().toISOString()
        };

        const fileId = await supabaseService.createFile(
            parseInt(userId),
            title || `${engine.toUpperCase()}: ${database}`,
            'application/sql',
            0,
            tableNames.length, // Total sheets = sequence count
            sourceInfo
        );

        // 2. Process each table
        for (let i = 0; i < tableNames.length; i++) {
            const tableName = tableNames[i];
            console.log(`  Processing table: ${tableName} (${i + 1}/${tableNames.length})`);

            const result = await dbConnectorService.getTableData(
                { engine, host, port, database, user, password },
                tableName
            );

            if (result.rows && result.rows.length > 0) {
                const rowCount = result.rows.length - 1;
                const columnCount = result.headers.length;

                // Create sheet for this table
                const sheetId = await supabaseService.createSheet(
                    fileId,
                    tableName,
                    i,
                    result.rows.length,
                    columnCount
                );

                // Insert row data
                const batchSize = 100;
                for (let rowIndex = 0; rowIndex < result.rows.length; rowIndex++) {
                    await supabaseService.createExcelData(sheetId, rowIndex, result.rows[rowIndex]);
                }

                importedResults.push({
                    id: sheetId,
                    name: tableName,
                    data: result.rows,
                    fileId: fileId // They all share the same fileId now
                });

                console.log(`    Imported ${rowCount} rows for ${tableName}`);
            }
        }

        if (importedResults.length === 0) {
            return res.status(400).json({ error: 'No data could be imported from the selected tables.' });
        }

        res.json({
            message: `Successfully imported ${importedResults.length} tables`,
            title: title || `${engine.toUpperCase()}: ${database}`,
            tables: importedResults
        });
    } catch (error) {
        console.error('SQL database multi-import error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/sql-db/refresh/:fileId', async (req, res) => {
    try {
        const fileId = parseInt(req.params.fileId);
        const { password } = req.body; // Password required for refresh

        const file = await supabaseService.getFileById(fileId);

        if (!file || !file.source_info || file.source_info.type !== 'sql_database') {
            return res.status(400).json({ error: 'File is not a SQL database import or missing source info' });
        }

        const { engine, host, port, database, user, table } = file.source_info;

        if (!password) {
            return res.status(400).json({ error: 'Password required to refresh SQL database connection' });
        }

        console.log(`Refreshing SQL database table: ${engine} ${host}:${port}/${database} - ${table}`);

        // 1. Fetch fresh data
        const result = await dbConnectorService.getTableData(
            { engine, host, port, database, user, password },
            table
        );

        if (!result.rows || result.rows.length === 0) {
            return res.status(400).json({ error: 'The table is now empty.' });
        }

        // 2. Update data in Supabase
        await supabaseService.updateFileData(fileId, [{ name: table, data: result.rows }]);

        // 3. Update sourceInfo with last refresh time
        const updatedSourceInfo = {
            ...file.source_info,
            lastRefresh: new Date().toISOString()
        };

        await supabaseService.supabase
            .from('uploaded_files')
            .update({ source_info: updatedSourceInfo })
            .eq('id', fileId);

        console.log(`Successfully refreshed table "${table}" with ${result.rows.length - 1} data rows`);

        res.json({
            message: 'SQL database refreshed successfully',
            rowCount: result.rows.length - 1,
            updatedAt: new Date().toISOString(),
            data: result.rows
        });
    } catch (error) {
        console.error('SQL database refresh error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// ============================================
// Admin: API Error Log Endpoints
// ============================================

/**
 * Report an API key error (called from frontend when Gemini calls fail)
 */
app.post('/api/admin/api-errors', async (req, res) => {
    try {
        const { error_type, error_message, source, key_index, user_id, user_email } = req.body;

        if (!error_type || !error_message || !source) {
            return res.status(400).json({ error: 'Missing required fields: error_type, error_message, source' });
        }

        await supabaseService.createApiErrorLog({
            error_type,
            error_message,
            source,
            key_index,
            user_id,
            user_email
        });

        console.log(`⚠️ [API Error Logged] ${error_type} in ${source}: ${error_message.substring(0, 100)}`);
        res.json({ message: 'Error logged successfully' });
    } catch (error) {
        console.error('Error logging API error:', error.message);
        // Still return 200 - error logging should not fail the client
        res.json({ message: 'Error logging attempted' });
    }
});

/**
 * Get all API error logs (admin)
 */
app.get('/api/admin/api-errors', async (req, res) => {
    try {
        const errors = await supabaseService.getApiErrorLogs();
        res.json(errors);
    } catch (error) {
        console.error('Get API errors error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

/**
 * Get unresolved API error count (for notification badge)
 */
app.get('/api/admin/api-errors/count', async (req, res) => {
    try {
        const count = await supabaseService.getUnresolvedApiErrorCount();
        res.json({ count });
    } catch (error) {
        console.error('Get API error count error:', error.message);
        res.json({ count: 0 });
    }
});

/**
 * Resolve a specific API error
 */
app.put('/api/admin/api-errors/:id/resolve', async (req, res) => {
    try {
        const id = parseInt(req.params.id);
        await supabaseService.resolveApiError(id);
        res.json({ message: 'Error resolved' });
    } catch (error) {
        console.error('Resolve API error:', error.message);
        res.status(500).json({ error: error.message });
    }
});

/**
 * Resolve all API errors
 */
app.put('/api/admin/api-errors/resolve-all', async (req, res) => {
    try {
        await supabaseService.resolveAllApiErrors();
        res.json({ message: 'All errors resolved' });
    } catch (error) {
        console.error('Resolve all API errors:', error.message);
        res.status(500).json({ error: error.message });
    }
});

/**
 * Clear resolved API errors
 */
app.delete('/api/admin/api-errors/resolved', async (req, res) => {
    try {
        await supabaseService.clearResolvedApiErrors();
        res.json({ message: 'Resolved errors cleared' });
    } catch (error) {
        console.error('Clear resolved API errors:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// ============================================
// Scheduled Refresh Endpoints
// ============================================

/**
 * Get refresh schedule for a dashboard
 */
app.get('/api/dashboards/:id/refresh-schedule', async (req, res) => {
    try {
        const dashboardId = parseInt(req.params.id);
        const schedule = await supabaseService.getRefreshSchedule(dashboardId);
        res.json(schedule || null);
    } catch (error) {
        console.error('Get refresh schedule error:', error);
        res.status(500).json({ error: error.message });
    }
});

/**
 * Create or update refresh schedule for a dashboard
 */
app.post('/api/dashboards/:id/refresh-schedule', async (req, res) => {
    try {
        const dashboardId = parseInt(req.params.id);
        const { userId, sourceType, sourceCredentials, refreshFrequency, refreshTimeUtc, refreshDay, timezone, refreshMonthDay } = req.body;

        if (!userId || !sourceType || !refreshFrequency || !refreshTimeUtc) {
            return res.status(400).json({ error: 'Missing required fields: userId, sourceType, refreshFrequency, refreshTimeUtc' });
        }

        // Verify user is admin or dashboard owner
        const { data: dashboard } = await supabaseService.supabase
            .from('dashboards')
            .select('user_id')
            .eq('id', dashboardId)
            .single();

        if (!dashboard) {
            return res.status(404).json({ error: 'Dashboard not found' });
        }

        const { data: user } = await supabaseService.supabase
            .from('users')
            .select('role, is_superuser')
            .eq('id', userId)
            .single();

        if (dashboard.user_id !== userId && user?.role !== 'ADMIN' && !user?.is_superuser) {
            return res.status(403).json({ error: 'Only dashboard owner or admin can set refresh schedule' });
        }

        const schedule = await supabaseService.createRefreshSchedule(
            dashboardId,
            userId,
            sourceType,
            sourceCredentials || {},
            refreshFrequency,
            refreshTimeUtc,
            refreshDay || null,
            timezone || 'Asia/Kolkata',
            refreshMonthDay || null
        );

        res.json(schedule);
    } catch (error) {
        console.error('Create refresh schedule error:', error);
        res.status(500).json({ error: error.message });
    }
});

/**
 * Delete refresh schedule for a dashboard
 */
app.delete('/api/dashboards/:id/refresh-schedule', async (req, res) => {
    try {
        const dashboardId = parseInt(req.params.id);
        const userId = parseInt(req.query.userId);

        if (!userId) return res.status(400).json({ error: 'Missing userId' });

        // Verify user is admin or dashboard owner
        const { data: dashboard } = await supabaseService.supabase
            .from('dashboards')
            .select('user_id')
            .eq('id', dashboardId)
            .single();

        if (!dashboard) return res.status(404).json({ error: 'Dashboard not found' });

        const { data: user } = await supabaseService.supabase
            .from('users')
            .select('role, is_superuser')
            .eq('id', userId)
            .single();

        if (dashboard.user_id !== userId && user?.role !== 'ADMIN' && !user?.is_superuser) {
            return res.status(403).json({ error: 'Only dashboard owner or admin can delete refresh schedule' });
        }

        await supabaseService.deleteRefreshSchedule(dashboardId);
        res.json({ message: 'Refresh schedule deleted' });
    } catch (error) {
        console.error('Delete refresh schedule error:', error);
        res.status(500).json({ error: error.message });
    }
});

/**
 * Test a data source connection for scheduled refresh
 */
app.post('/api/refresh-schedule/test-connection', async (req, res) => {
    try {
        const { sourceType, sourceCredentials } = req.body;

        if (!sourceType) {
            return res.status(400).json({ error: 'Missing sourceType' });
        }

        if (sourceType === 'google_sheet') {
            const { spreadsheetId, sheetNames } = sourceCredentials || {};
            if (!spreadsheetId) {
                return res.status(400).json({ error: 'Missing spreadsheetId' });
            }
            const metadata = await googleSheetsService.getMetadata(spreadsheetId);
            return res.json({ success: true, message: `Connected to "${metadata.title}"`, metadata });
        }

        if (sourceType === 'sql_database') {
            const result = await dbConnectorService.testConnection(sourceCredentials);
            return res.json(result);
        }

        if (sourceType === 'sharepoint') {
            // SharePoint uses service account - test connectivity
            if (!sharepointService.isConfigured()) {
                return res.json({ success: false, message: 'SharePoint is not configured on the server' });
            }
            await sharepointService.getAccessToken();
            return res.json({ success: true, message: 'SharePoint connection successful' });
        }

        return res.status(400).json({ error: `Unknown source type: ${sourceType}` });
    } catch (error) {
        console.error('Test connection error:', error);
        res.json({ success: false, message: error.message });
    }
});

/**
 * Manually trigger a refresh for a dashboard
 */
app.post('/api/dashboards/:id/refresh-now', async (req, res) => {
    try {
        const dashboardId = parseInt(req.params.id);
        const { userId } = req.body;

        if (!userId) return res.status(400).json({ error: 'Missing userId' });

        const schedule = await supabaseService.getRefreshSchedule(dashboardId);
        if (!schedule) return res.status(404).json({ error: 'No refresh schedule found for this dashboard' });

        // Execute refresh
        await executeRefresh(schedule);

        // Get updated schedule
        const updatedSchedule = await supabaseService.getRefreshSchedule(dashboardId);
        res.json({ message: 'Refresh completed', schedule: updatedSchedule });
    } catch (error) {
        console.error('Manual refresh error:', error);
        res.status(500).json({ error: error.message });
    }
});

// ============================================
// Scheduled Refresh Engine
// ============================================

/**
 * Execute a single refresh: fetch fresh data from source, update dashboard data_model
 */
async function executeRefresh(schedule) {
    const { dashboard_id, source_type, source_credentials } = schedule;
    console.log(`⏰ [Refresh] Executing refresh for dashboard ${dashboard_id} (source: ${source_type})`);

    // Mark as running
    await supabaseService.updateLastRefresh(dashboard_id, 'running');

    try {
        let freshData = null;
        let sheetsToProcess = [];

        // 1. Fetch raw data based on source type
        if (source_type === 'google_sheet') {
            const { spreadsheetId, sheetNames } = source_credentials;
            if (!spreadsheetId || !sheetNames || sheetNames.length === 0) {
                throw new Error('Missing spreadsheetId or sheetNames in credentials');
            }

            for (const sheetName of sheetNames) {
                const rows = await googleSheetsService.getSheetData(spreadsheetId, sheetName);
                console.log(`⏰ [Refresh] Fetched ${rows.length} rows from sheet "${sheetName}"`);
                sheetsToProcess.push({ name: sheetName, data: rows });
            }
        } else if (source_type === 'sql_database') {
            const { engine, host, port, user, password, database, tableName } = source_credentials;
            if (!engine || !host || !database || !user || !tableName) {
                throw new Error('Missing SQL connection credentials');
            }

            const tableData = await dbConnectorService.getTableData(
                { engine, host, port, user, password, database },
                tableName
            );
            
            if (tableData.rows && tableData.rows.length > 0) {
                sheetsToProcess.push({ name: tableName, data: tableData.rows });
            }
        } else if (source_type === 'sharepoint') {
            const { siteId, listId } = source_credentials;
            if (!siteId || !listId) {
                throw new Error('Missing SharePoint siteId or listId in credentials');
            }

            const result = await sharepointService.importList(siteId, listId);
            if (result.data && result.data.length > 0) {
                sheetsToProcess.push({ name: 'Sheet1', data: result.data });
            }
        } else {
            throw new Error(`Unsupported source type: ${source_type}`);
        }

        // 2. Process data and reconstruct DataModel
        const { data: dashboard } = await supabaseService.supabase
            .from('dashboards')
            .select('data_model')
            .eq('id', dashboard_id)
            .single();

        if (dashboard && dashboard.data_model && sheetsToProcess.length > 0) {
            const dataProcessing = require('./utils/dataProcessing');
            const dataModel = dashboard.data_model;
            let finalRows = [];

            // Handle Multi-sheet Join Scenario
            if (dataModel.joinConfigs && dataModel.joinConfigs.length > 0 && dataModel.tableConfigs) {
                const tables = [];
                Object.entries(dataModel.tableConfigs).forEach(([tableId, config]) => {
                    const refreshedSheet = sheetsToProcess.find(s => s.name === config.name);
                    if (refreshedSheet) {
                        tables.push({
                            id: tableId,
                            name: refreshedSheet.name,
                            rawData: { 
                                headers: refreshedSheet.data[config.headerIndex] || [], 
                                rows: refreshedSheet.data 
                            }
                        });
                    }
                });

                const headerIndices = {};
                Object.entries(dataModel.tableConfigs).forEach(([id, cfg]) => {
                    headerIndices[id] = cfg.headerIndex;
                });

                const joinResult = dataProcessing.performJoins(tables, dataModel.joinConfigs, headerIndices, dataModel.appendConfigs || []);
                finalRows = joinResult.data;
            } else {
                // Single-sheet fallback
                const headerIdx = dataModel.headerIndex || 0;
                const primarySheet = sheetsToProcess[0];
                const rawDataObj = {
                    headers: primarySheet.data[headerIdx] || [],
                    rows: primarySheet.data
                };
                const { rows } = dataProcessing.processRawData(rawDataObj, headerIdx);
                finalRows = rows;
            }

            // Map refreshed data to ProcessedRow objects maintaining strictly original columns
            const newProcessedData = finalRows.map(row => {
                const obj = {};
                dataModel.columns.forEach(col => {
                    const val = row[col];
                    if (dataModel.numericColumns && dataModel.numericColumns.includes(col)) {
                        const num = Number(val);
                        obj[col] = (val === '' || val === null || val === undefined || isNaN(num)) ? 0 : num;
                    } else {
                        obj[col] = (val === null || val === undefined) ? '' : String(val);
                    }
                });
                return obj;
            });

            freshData = { ...dataModel };
            freshData.data = newProcessedData;
        }

        if (freshData) {
            console.log(`⏰ [Refresh] Updating dashboard ${dashboard_id} with ${freshData.data ? freshData.data.length : 0} processed rows`);
            await supabaseService.updateDashboardDataModel(dashboard_id, freshData);
            await supabaseService.updateLastRefresh(dashboard_id, 'success');
            console.log(`✅ [Refresh] Dashboard ${dashboard_id} refreshed successfully`);
        } else {
            throw new Error('No fresh data could be fetched');
        }
    } catch (error) {
        console.error(`❌ [Refresh] Failed to refresh dashboard ${dashboard_id}:`, error.message);
        await supabaseService.updateLastRefresh(dashboard_id, 'failed', error.message);
    }
}

/**
 * Determines if a schedule is due for refresh based on its frequency and last refresh time
 */
function isScheduleDue(schedule) {
    const now = new Date();
    const lastRefresh = schedule.last_refreshed_at ? new Date(schedule.last_refreshed_at) : null;

    // Parse schedule time parts
    const [schedHours, schedMinutes] = (schedule.refresh_time_utc || '00:00').split(':').map(Number);
    const currentUTCHours = now.getUTCHours();
    const currentUTCMinutes = now.getUTCMinutes();
    const currentUTCDay = now.getUTCDay(); // 0=Sunday

    // Check if we are within the refresh window (within 2 minutes of scheduled time)
    const isInTimeWindow = currentUTCHours === schedHours && Math.abs(currentUTCMinutes - schedMinutes) <= 1;

    if (!lastRefresh) {
        // Never been refreshed, only run if in time window
        return isInTimeWindow;
    }

    const timeSinceLastRefresh = now.getTime() - lastRefresh.getTime();
    const hoursSinceLastRefresh = timeSinceLastRefresh / (1000 * 60 * 60);

    switch (schedule.refresh_frequency) {
        case 'hourly':
            return hoursSinceLastRefresh >= 1;
        case 'every_6_hours':
            return hoursSinceLastRefresh >= 6 && isInTimeWindow;
        case 'daily':
            return hoursSinceLastRefresh >= 23 && isInTimeWindow;
        case 'weekly':
            if (schedule.refresh_day !== null && schedule.refresh_day !== undefined) {
                return hoursSinceLastRefresh >= 167 && currentUTCDay === schedule.refresh_day && isInTimeWindow;
            }
            return hoursSinceLastRefresh >= 167 && isInTimeWindow;
        default:
            return null;
    }
}

function isScheduleDue(schedule) {
    const now = new Date();
    const lastTick = computeLastScheduledTick(schedule, now);
    if (!lastTick) return false;

    // The tick is always <= now by construction (we rewind if it was in the future).
    // Fire only if we haven't already refreshed at or after this tick.
    const lastRefresh = schedule.last_refreshed_at ? new Date(schedule.last_refreshed_at) : null;
    if (lastRefresh && lastRefresh.getTime() >= lastTick.getTime()) return false;

    // Catch-up grace window: if the tick is older than this and we never fired,
    // skip it to avoid surprise refreshes after long downtimes. The next tick
    // will fire normally.
    const GRACE_SECONDS = 60 * 60; // 1 hour
    const secondsLate = (now.getTime() - lastTick.getTime()) / 1000;
    if (secondsLate > GRACE_SECONDS) return false;

    return true;
}

/**
 * Server-side scheduler: polls every 60 seconds for due refresh schedules
 */
let schedulerRunning = false;
async function runScheduler() {
    if (schedulerRunning) return;
    schedulerRunning = true;

    try {
        const schedules = await supabaseService.getDueSchedules();
        console.log(`⏰ [Scheduler] Polled ${schedules.length} active schedule(s) at ${new Date().toISOString()}`);
        
        for (const schedule of schedules) {
            if (isScheduleDue(schedule)) {
                console.log(`⏰ [Scheduler] Dashboard ${schedule.dashboard_id} is due for refresh`);
                // Run refresh asynchronously (don't block the loop)
                executeRefresh(schedule).catch(err => {
                    console.error(`[Scheduler] Error refreshing dashboard ${schedule.dashboard_id}:`, err.message);
                });
            }
        }
    } catch (error) {
        console.error('[Scheduler] Error in scheduler loop:', error.message);
    } finally {
        schedulerRunning = false;
    }
}

// Start the scheduler: run immediately on startup, then every 30 seconds.
// A 30s cadence gives at-most-30s delay from the scheduled tick to execution.
const SCHEDULER_INTERVAL_MS = 30 * 1000;
runScheduler().catch(err => console.error('[Scheduler] Startup run failed:', err.message));
setInterval(runScheduler, SCHEDULER_INTERVAL_MS);
console.log(`⏰ Scheduled refresh engine started (polling every ${SCHEDULER_INTERVAL_MS / 1000}s)`);

app.listen(port, () => {
    console.log(`Server running on port ${port} with Supabase integration`);
    console.log('Supabase Configuration:');
    console.log(`  Project URL: ${supabaseService.supabaseUrl}`);
});
