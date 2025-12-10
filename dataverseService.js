const axios = require('axios');

class DataverseService {
    constructor() {
        // IMPORTANT: All credentials MUST be stored in .env file
        // NEVER hardcode credentials in source code!
        this.tenantId = process.env.TENANT_ID;
        this.clientId = process.env.CLIENT_ID;
        this.clientSecret = process.env.CLIENT_SECRET;
        this.resource = process.env.RESOURCE;
        this.baseUrl = process.env.BASE_URL;
        this.apiPath = process.env.DATAVERSE_API || '/api/data/v9.2';

        // Validate that all required environment variables are present
        const missingVars = [];
        if (!this.tenantId) missingVars.push('TENANT_ID');
        if (!this.clientId) missingVars.push('CLIENT_ID');
        if (!this.clientSecret) missingVars.push('CLIENT_SECRET');
        if (!this.resource) missingVars.push('RESOURCE');
        if (!this.baseUrl) missingVars.push('BASE_URL');

        if (missingVars.length > 0) {
            throw new Error(
                `Missing required environment variables: ${missingVars.join(', ')}. ` +
                'Please create a .env file with your Dataverse credentials. ' +
                'See .env.example for required variables.'
            );
        }

        this.accessToken = null;
        this.tokenExpiry = null;
    }

    async getAccessToken() {
        // Check if we have a valid cached token
        if (this.accessToken && this.tokenExpiry && Date.now() < this.tokenExpiry) {
            return this.accessToken;
        }

        try {
            const tokenUrl = `https://login.microsoftonline.com/${this.tenantId}/oauth2/token`;
            const params = new URLSearchParams();
            params.append('grant_type', 'client_credentials');
            params.append('client_id', this.clientId);
            params.append('client_secret', this.clientSecret);
            params.append('resource', this.resource);

            const response = await axios.post(tokenUrl, params, {
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            });

            this.accessToken = response.data.access_token;
            // Set expiry to 5 minutes before actual expiry
            this.tokenExpiry = Date.now() + ((response.data.expires_in - 300) * 1000);

            console.log('Successfully obtained Dataverse access token');
            return this.accessToken;
        } catch (error) {
            console.error('Error getting access token:', error.response?.data || error.message);
            throw new Error('Failed to authenticate with Dataverse');
        }
    }

    async makeRequest(method, endpoint, data = null, headers = {}) {
        const token = await this.getAccessToken();
        const url = `${this.baseUrl}${this.apiPath}${endpoint}`;

        const config = {
            method,
            url,
            headers: {
                'Authorization': `Bearer ${token}`,
                'OData-MaxVersion': '4.0',
                'OData-Version': '4.0',
                'Accept': 'application/json',
                'Content-Type': 'application/json; charset=utf-8',
                'Prefer': 'return=representation',
                ...headers
            }
        };

        if (data && (method === 'POST' || method === 'PATCH')) {
            config.data = data;
        }

        try {
            const response = await axios(config);
            return response.data;
        } catch (error) {
            console.error(`Dataverse API Error (${method} ${endpoint}):`, error.response?.data || error.message);
            throw error;
        }
    }

    // User Management Methods
    async getUsers() {
        const response = await this.makeRequest('GET', '/crc6f_hr_userses?$select=crc6f_id,crc6f_name,crc6f_email,crc6f_role,crc6f_created_at');
        return response.value.map(user => ({
            id: user.crc6f_id,
            name: user.crc6f_name,
            email: user.crc6f_email,
            role: user.crc6f_role,
            created_at: user.crc6f_created_at
        }));
    }

    async getUserByEmail(email) {
        const response = await this.makeRequest('GET', `/crc6f_hr_userses?$filter=crc6f_email eq '${email}'&$select=crc6f_id,crc6f_name,crc6f_email,crc6f_password,crc6f_role,crc6f_created_at`);
        if (response.value && response.value.length > 0) {
            const user = response.value[0];
            return {
                id: user.crc6f_id,
                name: user.crc6f_name,
                email: user.crc6f_email,
                password: user.crc6f_password,
                role: user.crc6f_role,
                created_at: user.crc6f_created_at
            };
        }
        return null;
    }

    async createUser(name, email, password, role = 'USER') {
        const data = {
            crc6f_name: name,
            crc6f_email: email,
            crc6f_password: password,
            crc6f_role: role,
            crc6f_created_at: new Date().toISOString()
        };

        const response = await this.makeRequest('POST', '/crc6f_hr_userses', data);
        return {
            id: response.crc6f_id,
            name: response.crc6f_name,
            email: response.crc6f_email,
            role: response.crc6f_role
        };
    }

    async deleteUser(userId) {
        const response = await this.makeRequest('GET', `/crc6f_hr_userses?$filter=crc6f_id eq ${userId}&$select=crc6f_hr_usersid`);
        if (response.value && response.value.length > 0) {
            const entityId = response.value[0].crc6f_hr_usersid;
            await this.makeRequest('DELETE', `/crc6f_hr_userses(${entityId})`);
            return true;
        }
        throw new Error('User not found');
    }

    // Dashboard Methods
    async createDashboard(userId, name, dataModel, chartConfigs) {
        console.log('Creating dashboard with params:', {
            userId,
            name,
            dataModelType: typeof dataModel,
            chartConfigsType: typeof chartConfigs
        });

        const data = {
            crc6f_user_id: userId,
            crc6f_name: name,
            crc6f_data_model: JSON.stringify(dataModel),
            crc6f_chart_configs: JSON.stringify(chartConfigs),
            crc6f_created_at: new Date().toISOString()
        };

        console.log('Sending dashboard data to Dataverse:', JSON.stringify(data, null, 2));

        try {
            const response = await this.makeRequest('POST', '/crc6f_hr_dashboardses', data);
            console.log('Dashboard created successfully:', response);
            return {
                id: response.crc6f_id,
                message: 'Dashboard saved'
            };
        } catch (error) {
            console.error('Error creating dashboard:', error.response?.data || error.message);
            console.error('Error details:', JSON.stringify(error.response?.data, null, 2));
            throw error;
        }
    }

    async getDashboardsByUser(userId) {
        const response = await this.makeRequest('GET', `/crc6f_hr_dashboardses?$filter=crc6f_user_id eq ${userId}&$select=crc6f_id,crc6f_name,crc6f_data_model,crc6f_chart_configs,crc6f_created_at&$orderby=crc6f_created_at desc`);
        return response.value.map(dashboard => ({
            id: dashboard.crc6f_id.toString(),
            name: dashboard.crc6f_name,
            date: new Date(dashboard.crc6f_created_at).toLocaleDateString(),
            dataModel: JSON.parse(dashboard.crc6f_data_model || '{}'),
            chartConfigs: JSON.parse(dashboard.crc6f_chart_configs || '[]')
        }));
    }

    async getAllDashboards() {
        // Get all dashboards
        const dashboardResponse = await this.makeRequest('GET', '/crc6f_hr_dashboardses?$select=crc6f_id,crc6f_name,crc6f_user_id,crc6f_data_model,crc6f_chart_configs,crc6f_created_at&$orderby=crc6f_created_at desc');

        // Get all users to map user_id to user_name
        const userResponse = await this.makeRequest('GET', '/crc6f_hr_userses?$select=crc6f_id,crc6f_name,crc6f_email');
        const userMap = {};
        userResponse.value.forEach(user => {
            userMap[user.crc6f_id] = {
                name: user.crc6f_name,
                email: user.crc6f_email
            };
        });

        return dashboardResponse.value.map(dashboard => ({
            id: dashboard.crc6f_id.toString(),
            name: dashboard.crc6f_name,
            user_name: userMap[dashboard.crc6f_user_id]?.name || 'Unknown',
            user_email: userMap[dashboard.crc6f_user_id]?.email || '',
            date: new Date(dashboard.crc6f_created_at).toLocaleDateString(),
            dataModel: JSON.parse(dashboard.crc6f_data_model || '{}'),
            chartConfigs: JSON.parse(dashboard.crc6f_chart_configs || '[]')
        }));
    }

    async deleteDashboard(dashboardId) {
        const response = await this.makeRequest('GET', `/crc6f_hr_dashboardses?$filter=crc6f_id eq ${dashboardId}&$select=crc6f_hr_dashboardsid`);
        if (response.value && response.value.length > 0) {
            const entityId = response.value[0].crc6f_hr_dashboardsid;
            await this.makeRequest('DELETE', `/crc6f_hr_dashboardses(${entityId})`);
            return true;
        }
        throw new Error('Dashboard not found');
    }

    // File Upload Methods
    async createFile(userId, originalName, mimeType, fileSize, sheetCount) {
        const data = {
            crc6f_user_id: userId,
            crc6f_original_name: originalName,
            crc6f_mime_type: mimeType,
            crc6f_file_size: fileSize,
            crc6f_sheet_count: sheetCount,
            crc6f_created_at: new Date().toISOString(),
            crc6f_updated_at: new Date().toISOString()
        };

        const response = await this.makeRequest('POST', '/crc6f_hr_uploaded_fileses', data);
        return response.crc6f_id;
    }

    async createSheet(fileId, sheetName, sheetIndex, rowCount, columnCount) {
        const data = {
            crc6f_file_id: fileId,
            crc6f_sheet_name: sheetName,
            crc6f_sheet_index: sheetIndex,
            crc6f_row_count: rowCount,
            crc6f_column_count: columnCount,
            crc6f_created_at: new Date().toISOString()
        };

        const response = await this.makeRequest('POST', '/crc6f_hr_excel_sheetses', data);
        return response.crc6f_id;
    }

    async createExcelData(sheetId, rowIndex, rowData) {
        const data = {
            crc6f_sheet_id: sheetId,
            crc6f_row_index: rowIndex,
            crc6f_row_data: JSON.stringify(rowData),
            crc6f_created_at: new Date().toISOString()
        };

        await this.makeRequest('POST', '/crc6f_hr_excel_datas', data);
    }

    async createFileUploadLog(fileId, uploadDate, uploadTime, filePath, status, errorMessage = null) {
        const data = {
            crc6f_file_id: fileId,
            crc6f_upload_date: uploadDate,
            crc6f_upload_time: uploadTime,
            crc6f_file_path: filePath,
            crc6f_status: status
        };

        if (errorMessage) {
            data.crc6f_error_message = errorMessage;
        }

        await this.makeRequest('POST', '/crc6f_hr_file_upload_logs', data);
    }

    async getAllUploads() {
        // Get all uploaded files
        const filesResponse = await this.makeRequest('GET', '/crc6f_hr_uploaded_fileses?$select=crc6f_id,crc6f_user_id,crc6f_original_name,crc6f_mime_type,crc6f_file_size,crc6f_sheet_count,crc6f_created_at&$orderby=crc6f_created_at desc');

        // Get all users to map user_id to user info
        const userResponse = await this.makeRequest('GET', '/crc6f_hr_userses?$select=crc6f_id,crc6f_name,crc6f_email');
        const userMap = {};
        userResponse.value.forEach(user => {
            userMap[user.crc6f_id] = {
                name: user.crc6f_name,
                email: user.crc6f_email
            };
        });

        // Get sheet counts and row counts for each file
        const sheetsResponse = await this.makeRequest('GET', '/crc6f_hr_excel_sheetses?$select=crc6f_file_id,crc6f_row_count');
        const fileStatsMap = {};
        sheetsResponse.value.forEach(sheet => {
            if (!fileStatsMap[sheet.crc6f_file_id]) {
                fileStatsMap[sheet.crc6f_file_id] = {
                    sheetCount: 0,
                    totalRows: 0
                };
            }
            fileStatsMap[sheet.crc6f_file_id].sheetCount++;
            fileStatsMap[sheet.crc6f_file_id].totalRows += sheet.crc6f_row_count || 0;
        });

        return filesResponse.value.map(file => ({
            id: file.crc6f_id,
            user_id: file.crc6f_user_id,
            filename: file.crc6f_original_name,
            original_name: file.crc6f_original_name,
            file_path: 'dataverse',
            mime_type: file.crc6f_mime_type,
            size: file.crc6f_file_size,
            created_at: file.crc6f_created_at,
            user_name: userMap[file.crc6f_user_id]?.name || 'Unknown',
            user_email: userMap[file.crc6f_user_id]?.email || '',
            sheet_count: fileStatsMap[file.crc6f_id]?.sheetCount || file.crc6f_sheet_count || 0,
            total_rows: fileStatsMap[file.crc6f_id]?.totalRows || 0
        }));
    }

    async getFileContent(fileId) {
        // Get file metadata
        const fileResponse = await this.makeRequest('GET', `/crc6f_hr_uploaded_fileses?$filter=crc6f_id eq ${fileId}&$select=crc6f_id,crc6f_original_name`);

        if (!fileResponse.value || fileResponse.value.length === 0) {
            throw new Error('File not found');
        }

        const file = fileResponse.value[0];

        // Get all sheets for this file
        const sheetsResponse = await this.makeRequest('GET', `/crc6f_hr_excel_sheetses?$filter=crc6f_file_id eq ${fileId}&$select=crc6f_id,crc6f_sheet_name,crc6f_sheet_index&$orderby=crc6f_sheet_index asc`);

        const sheets = [];

        // For each sheet, get its data
        for (const sheet of sheetsResponse.value) {
            const dataResponse = await this.makeRequest('GET', `/crc6f_hr_excel_datas?$filter=crc6f_sheet_id eq ${sheet.crc6f_id}&$select=crc6f_row_index,crc6f_row_data&$orderby=crc6f_row_index asc`);

            const sheetData = dataResponse.value.map(row => {
                try {
                    return JSON.parse(row.crc6f_row_data);
                } catch (e) {
                    console.error('Error parsing row data:', e);
                    return [];
                }
            });

            sheets.push({
                name: sheet.crc6f_sheet_name,
                data: sheetData
            });
        }

        return {
            fileName: file.crc6f_original_name,
            sheets: sheets
        };
    }

    async createDataConfigLog(fileName, columns, joinConfigs) {
        const now = new Date();
        const data = {
            crc6f_file_name: fileName,
            crc6f_columns: Array.isArray(columns) ? columns.join(', ') : columns,
            crc6f_join_configs: joinConfigs && joinConfigs.length > 0
                ? joinConfigs.map(j => `${j.leftTableId}.${j.leftKey} ${j.type} JOIN ${j.rightTableId}.${j.rightKey}`).join('; ')
                : 'no join configs',
            crc6f_config_date: now.toISOString().split('T')[0],
            crc6f_config_time: now.toTimeString().split(' ')[0],
            crc6f_created_at: now.toISOString()
        };

        await this.makeRequest('POST', '/crc6f_data_configuration_logs', data);
    }
}

module.exports = new DataverseService();
