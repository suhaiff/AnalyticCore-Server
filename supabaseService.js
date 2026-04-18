const { createClient } = require('@supabase/supabase-js');

class SupabaseService {
    constructor() {
        // IMPORTANT: All credentials MUST be stored in .env file
        // NEVER hardcode credentials in source code!
        this.supabaseUrl = process.env.SUPABASE_URL;
        this.supabaseKey = process.env.SUPABASE_KEY;

        // Validate that all required environment variables are present
        if (!this.supabaseUrl || !this.supabaseKey) {
            console.warn('⚠️  Supabase credentials missing or invalid. Database operations will not work.');
            console.warn('   Required env vars: SUPABASE_URL, SUPABASE_KEY');
            console.warn('   Please create a new Supabase project and update your .env file.');
            this.supabase = null;
            return;
        }

        // Initialize Supabase client
        try {
            this.supabase = createClient(this.supabaseUrl, this.supabaseKey);
            console.log('✓ Supabase client initialized successfully');
        } catch (error) {
            console.error('✗ Failed to initialize Supabase client:', error.message);
            this.supabase = null;
        }
    }

    // ==================== User Management ====================

    async getUsers() {
        try {
            const { data, error } = await this.supabase
                .from('users')
                .select('*, organizations(id, name)')
                .order('created_at', { ascending: false });

            if (error) throw error;
            return (data || []).map(u => ({
                ...u,
                organization_name: u.organizations?.name || null,
                organizations: undefined
            }));
        } catch (error) {
            console.error('Error fetching users:', error.message);
            throw error;
        }
    }

    async getUserByEmail(email) {
        try {
            const { data, error } = await this.supabase
                .from('users')
                .select('*')
                .eq('email', email)
                .single();

            if (error && error.code === 'PGRST116') {
                // No rows found - return null instead of throwing
                return null;
            }
            if (error) throw error;
            return data;
        } catch (error) {
            console.error('Error fetching user by email:', error.message);
            throw error;
        }
    }

    async createUser(name, email, password, role = 'USER', phone = null, company = null, job_title = null, domain = null, organization_id = null, is_superuser = false, must_change_password = false) {
        try {
            const insertData = {
                name,
                email,
                password,
                role,
                phone,
                company,
                job_title,
                domain,
                is_superuser: is_superuser || false,
                must_change_password: must_change_password || false,
                created_at: new Date().toISOString()
            };
            if (organization_id) insertData.organization_id = organization_id;

            const { data, error } = await this.supabase
                .from('users')
                .insert([insertData])
                .select()
                .single();

            if (error) throw error;

            // Don't return password
            const { password: _, ...userWithoutPassword } = data;
            return userWithoutPassword;
        } catch (error) {
            console.error('Error creating user:', error.message);
            throw error;
        }
    }

    async deleteUser(userId) {
        try {
            const { error } = await this.supabase
                .from('users')
                .delete()
                .eq('id', userId);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error deleting user:', error.message);
            throw error;
        }
    }

    async updateUserPassword(userId, newPassword, clearMustChange = true) {
        try {
            const updateData = { password: newPassword };
            if (clearMustChange) updateData.must_change_password = false;

            const { error } = await this.supabase
                .from('users')
                .update(updateData)
                .eq('id', userId);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error updating user password:', error.message);
            throw error;
        }
    }

    async setUserOtp(email, otpCode, expiresAt) {
        try {
            const { error } = await this.supabase
                .from('users')
                .update({ otp_code: otpCode, otp_expires_at: expiresAt })
                .eq('email', email);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error setting user OTP:', error.message);
            throw error;
        }
    }

    async clearUserOtp(email) {
        try {
            const { error } = await this.supabase
                .from('users')
                .update({ otp_code: null, otp_expires_at: null })
                .eq('email', email);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error clearing user OTP:', error.message);
            throw error;
        }
    }

    async updateUserPasswordByEmail(email, newPassword) {
        try {
            const { error } = await this.supabase
                .from('users')
                .update({ password: newPassword, must_change_password: false, otp_code: null, otp_expires_at: null })
                .eq('email', email);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error updating user password by email:', error.message);
            throw error;
        }
    }

    // ==================== Dashboard Management ====================

    async createDashboard(userId, name, dataModel, chartConfigs, sections = null, filterColumns = null, folderId = null, isWorkspace = false) {
        try {
            console.log('📊 Creating dashboard:', {
                userId,
                name,
                dataModelType: typeof dataModel,
                dataModelSize: JSON.stringify(dataModel).length,
                chartConfigsType: typeof chartConfigs,
                chartConfigsCount: chartConfigs?.length,
                chartConfigsSize: JSON.stringify(chartConfigs).length,
                sectionsCount: sections ? sections.length : 0,
                filterColumnsCount: filterColumns ? filterColumns.length : 0
            });

            // Store sections and filterColumns alongside chartConfigs in the JSONB column
            // This avoids needing a database migration to add new columns
            const chartConfigsWrapper = {
                charts: chartConfigs,
                sections: sections || [],
                filterColumns: filterColumns || []
            };

            const payload = {
                user_id: userId,
                name,
                data_model: dataModel,
                chart_configs: chartConfigsWrapper,
                created_at: new Date().toISOString(),
                folder_id: folderId || null,
                is_workspace: isWorkspace || false
            };

            const payloadSize = JSON.stringify(payload).length;
            console.log(`📦 Payload size: ${(payloadSize / 1024).toFixed(2)} KB`);

            if (payloadSize > 5 * 1024 * 1024) { // 5MB limit
                console.error('❌ Payload too large:', payloadSize);
                throw new Error(`Dashboard data too large (${(payloadSize / 1024 / 1024).toFixed(2)} MB). Please reduce the amount of data.`);
            }

            const { data, error } = await this.supabase
                .from('dashboards')
                .insert([payload])
                .select()
                .single();

            if (error) {
                console.error('❌ Supabase error:', {
                    message: error.message,
                    details: error.details,
                    hint: error.hint,
                    code: error.code
                });
                throw new Error(`Database error: ${error.message}${error.hint ? ` (${error.hint})` : ''}`);
            }

            console.log('✅ Dashboard created successfully:', { id: data.id, name: data.name });
            return {
                id: data.id,
                message: 'Dashboard saved'
            };
        } catch (error) {
            console.error('❌ Error creating dashboard:', {
                message: error.message,
                stack: error.stack,
                name: error.name
            });
            throw error;
        }
    }

    async updateDashboard(dashboardId, name, dataModel, chartConfigs, sections = null, filterColumns = null, folderId = null, isWorkspace = false) {
        try {
            const chartConfigsWrapper = {
                charts: chartConfigs,
                sections: sections || [],
                filterColumns: filterColumns || []
            };

            const { data, error } = await this.supabase
                .from('dashboards')
                .update({
                    name,
                    data_model: dataModel,
                    chart_configs: chartConfigsWrapper,
                    folder_id: folderId || null,
                    is_workspace: isWorkspace || false
                })
                .eq('id', dashboardId)
                .select()
                .single();

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('Error updating dashboard:', error.message);
            throw error;
        }
    }

    async getDashboardById(dashboardId) {
        try {
            const { data: dashboard, error } = await this.supabase
                .from('dashboards')
                .select('*')
                .eq('id', dashboardId)
                .single();

            if (error) throw error;
            if (!dashboard) return null;

            const raw = dashboard.chart_configs;
            let chartConfigs, sections, filterColumns;
            if (raw && !Array.isArray(raw) && raw.charts) {
                chartConfigs = raw.charts || [];
                sections = raw.sections || [];
                filterColumns = raw.filterColumns || [];
            } else {
                chartConfigs = raw || [];
                sections = [];
                filterColumns = [];
            }

            return {
                id: dashboard.id.toString(),
                name: dashboard.name,
                date: new Date(dashboard.created_at).toLocaleDateString(),
                dataModel: dashboard.data_model || {},
                chartConfigs,
                sections,
                filterColumns,
                folder_id: dashboard.folder_id || null,
                is_workspace: dashboard.is_workspace || false,
                updated_at: dashboard.updated_at || dashboard.created_at
            };
        } catch (error) {
            console.error('Error fetching dashboard by id:', error.message);
            throw error;
        }
    }

    async getDashboardsByUser(userId) {
        try {
            const { data, error } = await this.supabase
                .from('dashboards')
                .select('*')
                .eq('user_id', userId)
                .eq('is_workspace', false)
                .order('created_at', { ascending: false });

            if (error) throw error;

            return (data || []).map(dashboard => {
                // Handle both old format (plain array) and new format (wrapper object)
                const raw = dashboard.chart_configs;
                let chartConfigs, sections, filterColumns;

                if (raw && !Array.isArray(raw) && raw.charts) {
                    // New wrapper format
                    chartConfigs = raw.charts || [];
                    sections = raw.sections || [];
                    filterColumns = raw.filterColumns || [];
                } else {
                    // Legacy format: chart_configs is a plain array
                    chartConfigs = raw || [];
                    sections = [];
                    filterColumns = [];
                }

                return {
                    id: dashboard.id.toString(),
                    name: dashboard.name,
                    date: new Date(dashboard.created_at).toLocaleDateString(),
                    dataModel: dashboard.data_model || {},
                    chartConfigs,
                    sections,
                    filterColumns,
                    folder_id: dashboard.folder_id || null,
                    is_workspace: dashboard.is_workspace || false,
                    // Include user_id so the client can evaluate ownership (Edit/Save/Refresh
                    // button visibility depends on comparing this to the current user's id).
                    user_id: dashboard.user_id
                };
            });
        } catch (error) {
            console.error('Error fetching dashboards by user:', error.message);
            throw error;
        }
    }

    async getAllDashboards() {
        try {
            // Get all dashboards with user information
            const { data, error } = await this.supabase
                .from('dashboards')
                .select(`
                    *,
                    users(id, name, email)
                `)
                .order('created_at', { ascending: false });

            if (error) throw error;

            return (data || []).map(dashboard => {
                const raw = dashboard.chart_configs;
                let chartConfigs, sections, filterColumns;

                if (raw && !Array.isArray(raw) && raw.charts) {
                    chartConfigs = raw.charts || [];
                    sections = raw.sections || [];
                    filterColumns = raw.filterColumns || [];
                } else {
                    chartConfigs = raw || [];
                    sections = [];
                    filterColumns = [];
                }

                return {
                    id: dashboard.id.toString(),
                    name: dashboard.name,
                    user_id: dashboard.user_id,
                    user_name: dashboard.users?.name || 'Unknown',
                    user_email: dashboard.users?.email || '',
                    date: new Date(dashboard.created_at).toLocaleDateString(),
                    dataModel: dashboard.data_model || {},
                    chartConfigs,
                    sections,
                    filterColumns
                };
            });
        } catch (error) {
            console.error('Error fetching all dashboards:', error.message);
            throw error;
        }
    }

    async deleteDashboard(dashboardId) {
        try {
            const { error } = await this.supabase
                .from('dashboards')
                .delete()
                .eq('id', dashboardId);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error deleting dashboard:', error.message);
            throw error;
        }
    }

    // ==================== File Upload Management ====================

    async createFile(userId, originalName, mimeType, fileSize, sheetCount, sourceInfo = null) {
        try {
            const { data, error } = await this.supabase
                .from('uploaded_files')
                .insert([
                    {
                        user_id: userId,
                        original_name: originalName,
                        mime_type: mimeType,
                        file_size: fileSize,
                        sheet_count: sheetCount,
                        source_info: sourceInfo,
                        created_at: new Date().toISOString(),
                        updated_at: new Date().toISOString()
                    }
                ])
                .select()
                .single();

            if (error) throw error;
            return data.id;
        } catch (error) {
            console.error('Error creating file record:', error.message);
            throw error;
        }
    }

    async createSheet(fileId, sheetName, sheetIndex, rowCount, columnCount) {
        try {
            const { data, error } = await this.supabase
                .from('excel_sheets')
                .insert([
                    {
                        file_id: fileId,
                        sheet_name: sheetName,
                        sheet_index: sheetIndex,
                        row_count: rowCount,
                        column_count: columnCount,
                        created_at: new Date().toISOString()
                    }
                ])
                .select()
                .single();

            if (error) throw error;
            return data.id;
        } catch (error) {
            console.error('Error creating sheet record:', error.message);
            throw error;
        }
    }

    async createExcelData(sheetId, rowIndex, rowData) {
        try {
            const { error } = await this.supabase
                .from('excel_data')
                .insert([
                    {
                        sheet_id: sheetId,
                        row_index: rowIndex,
                        row_data: rowData,
                        created_at: new Date().toISOString()
                    }
                ]);

            if (error) throw error;
        } catch (error) {
            console.error('Error creating excel data record:', error.message);
            throw error;
        }
    }

    async createExcelDataBatch(sheetId, rows) {
        try {
            // rows is an array of {rowIndex, rowData}
            const insertData = rows.map(row => ({
                sheet_id: sheetId,
                row_index: row.rowIndex,
                row_data: row.rowData,
                created_at: new Date().toISOString()
            }));

            const { error } = await this.supabase
                .from('excel_data')
                .insert(insertData);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error batch creating excel data:', error.message);
            throw error;
        }
    }

    async createFileUploadLog(fileId, uploadDate, uploadTime, filePath, status, errorMessage = null) {
        try {
            const logData = {
                file_id: fileId,
                upload_date: uploadDate,
                upload_time: uploadTime,
                file_path: filePath,
                status,
                created_at: new Date().toISOString()
            };

            if (errorMessage) {
                logData.error_message = errorMessage;
            }

            const { error } = await this.supabase
                .from('file_upload_logs')
                .insert([logData]);

            if (error) {
                console.warn('⚠️ Supabase logging failed (database level):', error.message);
                return false;
            }
            return true;
        } catch (error) {
            console.warn('⚠️ Supabase logging failed (connection level): Check your network or project credentials. Error:', error.message);
            return false;
        }
    }

    async getAllUploads() {
        try {
            // Get all uploaded files with user and sheet information
            const { data: files, error: filesError } = await this.supabase
                .from('uploaded_files')
                .select(`
                    *,
                    users(id, name, email)
                `)
                .order('created_at', { ascending: false });

            if (filesError) throw filesError;

            // Get sheet information for each file
            const { data: sheets, error: sheetsError } = await this.supabase
                .from('excel_sheets')
                .select('file_id, row_count');

            if (sheetsError) throw sheetsError;

            // Build a map of file stats
            const fileStatsMap = {};
            (sheets || []).forEach(sheet => {
                if (!fileStatsMap[sheet.file_id]) {
                    fileStatsMap[sheet.file_id] = {
                        sheetCount: 0,
                        totalRows: 0
                    };
                }
                fileStatsMap[sheet.file_id].sheetCount++;
                fileStatsMap[sheet.file_id].totalRows += sheet.row_count || 0;
            });

            return (files || []).map(file => ({
                id: file.id,
                user_id: file.user_id,
                filename: file.original_name,
                original_name: file.original_name,
                file_path: 'supabase',
                mime_type: file.mime_type,
                size: file.file_size,
                created_at: file.created_at,
                user_name: file.users?.name || 'Unknown',
                user_email: file.users?.email || '',
                sheet_count: fileStatsMap[file.id]?.sheetCount || file.sheet_count || 0,
                total_rows: fileStatsMap[file.id]?.totalRows || 0
            }));
        } catch (error) {
            console.error('Error fetching all uploads:', error.message);
            throw error;
        }
    }

    async getFileContent(fileId) {
        try {
            // Get file metadata
            const { data: file, error: fileError } = await this.supabase
                .from('uploaded_files')
                .select('*')
                .eq('id', fileId)
                .single();

            if (fileError) throw fileError;
            if (!file) throw new Error('File not found');

            // Get all sheets for this file
            const { data: sheetsData, error: sheetsError } = await this.supabase
                .from('excel_sheets')
                .select('*')
                .eq('file_id', fileId)
                .order('sheet_index', { ascending: true });

            if (sheetsError) throw sheetsError;

            const sheets = [];

            // For each sheet, get its data
            for (const sheet of (sheetsData || [])) {
                const { data: excelData, error: dataError } = await this.supabase
                    .from('excel_data')
                    .select('*')
                    .eq('sheet_id', sheet.id)
                    .order('row_index', { ascending: true });

                if (dataError) throw dataError;

                const sheetData = (excelData || []).map(row => {
                    // Robust check: row_data might be stored as a stringified JSON if column type is text
                    if (typeof row.row_data === 'string') {
                        try {
                            return JSON.parse(row.row_data);
                        } catch (e) {
                            console.error('Failed to parse row_data string:', row.row_data);
                            return [];
                        }
                    }
                    return row.row_data || [];
                });

                sheets.push({
                    name: sheet.sheet_name,
                    data: sheetData
                });
            }

            return {
                fileName: file.original_name,
                sheets: sheets
            };
        } catch (error) {
            console.error('Error fetching file content:', error.message);
            throw error;
        }
    }

    async createDataConfigLog(fileName, columns, joinConfigs) {
        try {
            const now = new Date();
            const joinConfigString = (joinConfigs && joinConfigs.length > 0)
                ? joinConfigs.map(j => `${j.leftTableId}.${j.leftKey} ${j.type} JOIN ${j.rightTableId}.${j.rightKey}`).join('; ')
                : 'no join configs';

            const { error } = await this.supabase
                .from('data_configuration_logs')
                .insert([
                    {
                        file_name: fileName,
                        columns: Array.isArray(columns) ? columns.join(', ') : columns,
                        join_configs: joinConfigString,
                        config_date: now.toISOString().split('T')[0],
                        config_time: now.toTimeString().split(' ')[0],
                        created_at: now.toISOString()
                    }
                ]);

            if (error) {
                console.warn('⚠️ Supabase logging failed (database level):', error.message);
                return false;
            }
            return true;
        } catch (error) {
            console.warn('⚠️ Supabase logging failed (connection level): Check your network or project credentials. Error:', error.message);
            return false;
        }
    }

    async getFileById(fileId) {
        try {
            const { data, error } = await this.supabase
                .from('uploaded_files')
                .select('*')
                .eq('id', fileId)
                .single();

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('Error fetching file by ID:', error.message);
            throw error;
        }
    }

    async updateFileData(fileId, sheets) {
        try {
            // 1. Get existing sheets to delete their data
            const { data: existingSheets, error: fetchError } = await this.supabase
                .from('excel_sheets')
                .select('id')
                .eq('file_id', fileId);

            if (fetchError) throw fetchError;

            // 2. Delete existing sheets (Cascade will handle excel_data)
            if (existingSheets && existingSheets.length > 0) {
                const { error: deleteError } = await this.supabase
                    .from('excel_sheets')
                    .delete()
                    .eq('file_id', fileId);

                if (deleteError) throw deleteError;
            }

            // 3. Insert new sheets and data
            for (let i = 0; i < sheets.length; i++) {
                const { name, data } = sheets[i];
                const rowCount = data.length;
                const columnCount = rowCount > 0 ? Math.max(...data.map(row => row.length)) : 0;

                const sheetId = await this.createSheet(fileId, name, i, rowCount, columnCount);

                // Insert in batches
                const batchSize = 100;
                for (let rowIndex = 0; rowIndex < data.length; rowIndex++) {
                    await this.createExcelData(sheetId, rowIndex, data[rowIndex]);
                }
            }

            // 4. Update updated_at
            await this.supabase
                .from('uploaded_files')
                .update({ updated_at: new Date().toISOString() })
                .eq('id', fileId);

            return true;
        } catch (error) {
            console.error('Error updating file data:', error.message);
            throw error;
        }
    }
    // ==================== API Error Logs ====================

    async createApiErrorLog(errorData) {
        try {
            const { error } = await this.supabase
                .from('api_error_logs')
                .insert([{
                    error_type: errorData.error_type,
                    error_message: errorData.error_message,
                    source: errorData.source,
                    key_index: errorData.key_index || null,
                    user_id: errorData.user_id || null,
                    user_email: errorData.user_email || null,
                    resolved: false,
                    created_at: new Date().toISOString()
                }]);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error creating API error log:', error.message);
            // Don't throw - error logging should never break the application
            return false;
        }
    }

    async getApiErrorLogs() {
        try {
            const { data, error } = await this.supabase
                .from('api_error_logs')
                .select('*')
                .order('created_at', { ascending: false })
                .limit(100);

            if (error) throw error;
            return data || [];
        } catch (error) {
            console.error('Error fetching API error logs:', error.message);
            return [];
        }
    }

    async getUnresolvedApiErrorCount() {
        try {
            const { count, error } = await this.supabase
                .from('api_error_logs')
                .select('*', { count: 'exact', head: true })
                .eq('resolved', false);

            if (error) throw error;
            return count || 0;
        } catch (error) {
            console.error('Error fetching unresolved error count:', error.message);
            return 0;
        }
    }

    async resolveApiError(id) {
        try {
            const { error } = await this.supabase
                .from('api_error_logs')
                .update({ resolved: true })
                .eq('id', id);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error resolving API error:', error.message);
            throw error;
        }
    }

    async resolveAllApiErrors() {
        try {
            const { error } = await this.supabase
                .from('api_error_logs')
                .update({ resolved: true })
                .eq('resolved', false);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error resolving all API errors:', error.message);
            throw error;
        }
    }

    async clearResolvedApiErrors() {
        try {
            const { error } = await this.supabase
                .from('api_error_logs')
                .delete()
                .eq('resolved', true);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error clearing resolved API errors:', error.message);
            throw error;
        }
    }

    // ==================== Workspace Folder Management ====================

    async createWorkspaceFolder(ownerId, name, accessUsers = [], accessGroups = []) {
        try {
            // Create folder
            const { data: folder, error: folderError } = await this.supabase
                .from('workspace_folders')
                .insert([{
                    name,
                    owner_id: ownerId.toString(),
                    created_at: new Date().toISOString()
                }])
                .select()
                .single();

            if (folderError) throw folderError;

            // Add user access entries
            const nonOwnerUsers = accessUsers.filter(u => u.id.toString() !== ownerId.toString());
            if (nonOwnerUsers.length > 0) {
                const accessRows = nonOwnerUsers.map(u => ({
                    folder_id: folder.id,
                    user_id: u.id.toString(),
                    access_level: u.level || 'VIEWER'
                }));
                const { error: accessError } = await this.supabase
                    .from('workspace_folder_access')
                    .insert(accessRows);
                if (accessError) throw accessError;
            }

            // Add group access entries
            if (accessGroups && accessGroups.length > 0) {
                const groupAccessRows = accessGroups.map(g => ({
                    folder_id: folder.id,
                    group_id: g.id,
                    access_level: g.level || 'VIEWER'
                }));
                const { error: groupAccessError } = await this.supabase
                    .from('workspace_folder_group_access')
                    .insert(groupAccessRows);
                if (groupAccessError) throw groupAccessError;
            }

            console.log('✅ Workspace folder created with levels:', folder.id);
            return folder;
        } catch (error) {
            console.error('Error creating workspace folder:', error.message);
            throw error;
        }
    }

    async getAccessibleFolders(userId) {
        try {
            const userIdStr = userId.toString();

            // 1. Folders owned by user
            const { data: ownedFolders, error: ownedError } = await this.supabase
                .from('workspace_folders')
                .select('*')
                .eq('owner_id', userIdStr)
                .order('created_at', { ascending: false });

            if (ownedError) throw ownedError;

            // 2. Folders shared with user via user access table
            const { data: accessRows, error: accessError } = await this.supabase
                .from('workspace_folder_access')
                .select('folder_id')
                .eq('user_id', userIdStr);

            if (accessError) throw accessError;

            // 2.5 Folders shared with user via group access table
            const { data: userGroups, error: userGroupsError } = await this.supabase
                .from('workspace_group_members')
                .select('group_id')
                .eq('user_id', userIdStr);
            
            if (userGroupsError) throw userGroupsError;

            let groupFolderIds = [];
            if (userGroups && userGroups.length > 0) {
                const groupIds = userGroups.map(ug => ug.group_id);
                const { data: groupAccessRows, error: groupAccessError } = await this.supabase
                    .from('workspace_folder_group_access')
                    .select('folder_id')
                    .in('group_id', groupIds);
                
                if (groupAccessError) throw groupAccessError;
                groupFolderIds = (groupAccessRows || []).map(r => r.folder_id);
            }

            let sharedFolders = [];
            const sharedFolderIds = [
                ...((accessRows || []).map(r => r.folder_id)),
                ...groupFolderIds
            ];

            if (sharedFolderIds.length > 0) {
                const { data: sf, error: sfError } = await this.supabase
                    .from('workspace_folders')
                    .select('*')
                    .in('id', sharedFolderIds)
                    .order('created_at', { ascending: false });

                if (sfError) throw sfError;
                sharedFolders = sf || [];
            }

            // 3. Merge, deduplicate by id
            const allFolders = [...(ownedFolders || []), ...sharedFolders];
            const seen = new Set();
            const uniqueFolders = [];

            for (const f of allFolders) {
                if (!seen.has(f.id)) {
                    seen.add(f.id);
                    
                    // Attach effective role
                    const isOwner = f.owner_id && f.owner_id.toString() === userIdStr;
                    let role = isOwner ? 'ADMIN' : 'VIEWER';

                    if (!isOwner) {
                        // We need to fetch the highest role for this folder from both direct and group access
                        // Check direct user access role
                        const { data: directAccess } = await this.supabase
                            .from('workspace_folder_access')
                            .select('access_level')
                            .eq('folder_id', f.id)
                            .eq('user_id', userIdStr)
                            .single();
                        
                        if (directAccess) role = directAccess.access_level;

                        // Check group access roles
                        if (userGroups && userGroups.length > 0) {
                            const groupIds = userGroups.map(ug => ug.group_id);
                            const { data: groupAccess } = await this.supabase
                                .from('workspace_folder_group_access')
                                .select('access_level')
                                .eq('folder_id', f.id)
                                .in('group_id', groupIds);
                            
                            if (groupAccess && groupAccess.length > 0) {
                                const rolePriority = { 'ADMIN': 3, 'EDITOR': 2, 'VIEWER': 1 };
                                for (const ga of groupAccess) {
                                    if (rolePriority[ga.access_level] > rolePriority[role]) {
                                        role = ga.access_level;
                                    }
                                }
                            }
                        }
                    }

                    uniqueFolders.push({
                        ...f,
                        is_owner: isOwner,
                        effective_level: role
                    });
                }
            }

            // 4. For each folder, fetch access user and group details
            const foldersWithAccess = await Promise.all(uniqueFolders.map(async (folder) => {
                // Fetch User access
                const { data: fAccess } = await this.supabase
                    .from('workspace_folder_access')
                    .select('user_id, access_level')
                    .eq('folder_id', folder.id);

                const accessUserMap = (fAccess || []).reduce((acc, r) => {
                    acc[r.user_id] = r.access_level;
                    return acc;
                }, {});

                const accessUserIds = Object.keys(accessUserMap);
                let accessUsers = [];
                if (accessUserIds.length > 0) {
                    const { data: users } = await this.supabase
                        .from('users')
                        .select('id, name, email')
                        .in('id', accessUserIds);
                    accessUsers = (users || []).map(u => ({ 
                        id: u.id, 
                        name: u.name, 
                        email: u.email,
                        level: accessUserMap[u.id] || 'VIEWER'
                    }));
                }

                // Fetch Group access
                const { data: fgAccess } = await this.supabase
                    .from('workspace_folder_group_access')
                    .select('group_id, access_level')
                    .eq('folder_id', folder.id);
                
                const accessGroupMap = (fgAccess || []).reduce((acc, r) => {
                    acc[r.group_id] = r.access_level;
                    return acc;
                }, {});

                const accessGroupIds = Object.keys(accessGroupMap);
                let accessGroups = [];
                if (accessGroupIds.length > 0) {
                    const { data: groups } = await this.supabase
                        .from('workspace_groups')
                        .select('id, name')
                        .in('id', accessGroupIds);
                    accessGroups = (groups || []).map(g => ({ 
                        id: g.id, 
                        name: g.name,
                        level: accessGroupMap[g.id] || 'VIEWER'
                    }));
                }

                return {
                    id: folder.id,
                    name: folder.name,
                    owner_id: folder.owner_id,
                    created_at: folder.created_at,
                    access_users: accessUsers,
                    access_groups: accessGroups,
                    is_owner: folder.owner_id && folder.owner_id.toString() === userIdStr
                };
            }));

            return foldersWithAccess;
        } catch (error) {
            console.error('Error fetching accessible folders:', error.message);
            throw error;
        }
    }

    async updateWorkspaceFolder(folderId, name, accessUsers = [], accessGroups = [], requestingUserId) {
        try {
            // Verify ownership
            const { data: folder, error: fetchError } = await this.supabase
                .from('workspace_folders')
                .select('owner_id')
                .eq('id', folderId)
                .single();

            if (fetchError) throw fetchError;
            if (!folder) throw new Error('Folder not found');

            const isOwner = folder && folder.owner_id && folder.owner_id.toString() === requestingUserId.toString();

            if (!isOwner) {
                // FUTURE: Check for ADMIN level here if we want to allow admins to manage access
                throw new Error('Only the folder owner can update it');
            }

            // Update name
            const { error: updateError } = await this.supabase
                .from('workspace_folders')
                .update({ name })
                .eq('id', folderId);

            if (updateError) throw updateError;

            // Replace user access list
            await this.supabase
                .from('workspace_folder_access')
                .delete()
                .eq('folder_id', folderId);

            const nonOwnerUsers = accessUsers.filter(u => u.id.toString() !== requestingUserId.toString());
            if (nonOwnerUsers.length > 0) {
                const accessRows = nonOwnerUsers.map(u => ({
                    folder_id: folderId,
                    user_id: u.id.toString(),
                    access_level: u.level || 'VIEWER'
                }));
                const { error: accessError } = await this.supabase
                    .from('workspace_folder_access')
                    .insert(accessRows);
                if (accessError) throw accessError;
            }

            // Replace group access list
            await this.supabase
                .from('workspace_folder_group_access')
                .delete()
                .eq('folder_id', folderId);

            if (accessGroups && accessGroups.length > 0) {
                const groupAccessRows = accessGroups.map(g => ({
                    folder_id: folderId,
                    group_id: g.id,
                    access_level: g.level || 'VIEWER'
                }));
                const { error: groupAccessError } = await this.supabase
                    .from('workspace_folder_group_access')
                    .insert(groupAccessRows);
                if (groupAccessError) throw groupAccessError;
            }

            console.log('✅ Workspace folder updated with levels:', folderId);
            return true;
        } catch (error) {
            console.error('Error updating workspace folder:', error.message);
            throw error;
        }
    }

    async deleteWorkspaceFolder(folderId, requestingUserId) {
        try {
            // Verify ownership
            const { data: folder, error: fetchError } = await this.supabase
                .from('workspace_folders')
                .select('owner_id')
                .eq('id', folderId)
                .single();

            if (fetchError) throw fetchError;
            if (!folder) throw new Error('Folder not found');

            console.log('--- Workspace Delete Ownership Check ---');
            console.log('Folder ID:', folderId);
            console.log('Folder owner_id:', folder.owner_id, typeof folder.owner_id);
            console.log('Requesting userId:', requestingUserId, typeof requestingUserId);

            const isOwner = folder && folder.owner_id && folder.owner_id.toString() === requestingUserId.toString();
            console.log('Is Owner?', isOwner);

            if (!isOwner) {
                throw new Error('Only the folder owner can delete it');
            }

            // Nullify dashboards referencing this folder
            await this.supabase
                .from('dashboards')
                .update({ folder_id: null, is_workspace: false })
                .eq('folder_id', folderId);

            // Delete folder (access rows cascade)
            const { error: deleteError } = await this.supabase
                .from('workspace_folders')
                .delete()
                .eq('id', folderId);

            if (deleteError) throw deleteError;

            console.log('✅ Workspace folder deleted:', folderId);
            return true;
        } catch (error) {
            console.error('Error deleting workspace folder:', error.message);
            throw error;
        }
    }

    async getFolderDashboards(folderId, requestingUserId) {
        try {
            // Check access: must be owner or in access table
            const { data: folder } = await this.supabase
                .from('workspace_folders')
                .select('owner_id')
                .eq('id', folderId)
                .single();

            const isOwner = folder && folder.owner_id && folder.owner_id.toString() === requestingUserId.toString();
            let effectiveLevel = isOwner ? 'ADMIN' : null;

            if (!isOwner) {
                // Check direct user access
                const { data: access } = await this.supabase
                    .from('workspace_folder_access')
                    .select('access_level')
                    .eq('folder_id', folderId)
                    .eq('user_id', requestingUserId.toString())
                    .single();

                if (access) {
                    effectiveLevel = access.access_level;
                }

                // Check group access
                const { data: userGroups } = await this.supabase
                    .from('workspace_group_members')
                    .select('group_id')
                    .eq('user_id', requestingUserId.toString());
                
                if (userGroups && userGroups.length > 0) {
                    const groupIds = userGroups.map(ug => ug.group_id);
                    const { data: groupAccesses } = await this.supabase
                        .from('workspace_folder_group_access')
                        .select('access_level')
                        .eq('folder_id', folderId)
                        .in('group_id', groupIds);
                    
                    if (groupAccesses && groupAccesses.length > 0) {
                        const levels = ['VIEWER', 'EDITOR', 'ADMIN'];
                        groupAccesses.forEach(ga => {
                            if (!effectiveLevel || levels.indexOf(ga.access_level) > levels.indexOf(effectiveLevel)) {
                                effectiveLevel = ga.access_level;
                            }
                        });
                    }
                }

                if (!effectiveLevel) {
                    throw new Error('Access denied to this folder');
                }
            }

            const { data, error } = await this.supabase
                .from('dashboards')
                .select(`
                    *,
                    users(id, name, email)
                `)

                .eq('folder_id', folderId)
                .eq('is_workspace', true)
                .order('created_at', { ascending: false });

            if (error) throw error;

            // Fetch specific individual access levels for these dashboards for this user
            const dashboardIds = (data || []).map(d => d.id);
            let userAccessMap = {};
            
            if (dashboardIds.length > 0) {
                const { data: accessData } = await this.supabase
                    .from('dashboard_access')
                    .select('dashboard_id, access_level')
                    .in('dashboard_id', dashboardIds)
                    .eq('user_id', requestingUserId);
                
                if (accessData) {
                    accessData.forEach(acc => {
                        userAccessMap[acc.dashboard_id] = acc.access_level;
                    });
                }
            }

            const dashboards = (data || []).map(dashboard => {
                const raw = dashboard.chart_configs;
                let chartConfigs, sections, filterColumns;

                if (raw && !Array.isArray(raw) && raw.charts) {
                    chartConfigs = raw.charts || [];
                    sections = raw.sections || [];
                    filterColumns = raw.filterColumns || [];
                } else {
                    chartConfigs = raw || [];
                    sections = [];
                    filterColumns = [];
                }

                return {
                    id: dashboard.id.toString(),
                    name: dashboard.name,
                    date: new Date(dashboard.created_at).toLocaleDateString(),
                    dataModel: dashboard.data_model || {},
                    chartConfigs,
                    sections,
                    filterColumns,
                    folder_id: dashboard.folder_id || null,
                    is_workspace: dashboard.is_workspace || false,
                    user_id: dashboard.user_id,
                    owner_name: dashboard.users?.name || 'Unknown',
                    shared_access_level: userAccessMap[dashboard.id] || null
                };
            });


            return {
                dashboards,
                effectiveLevel
            };
        } catch (error) {
            console.error('Error fetching folder dashboards:', error.message);
            throw error;
        }
    }

    // ==================== Workspace Group Management ====================

    async createWorkspaceGroup(ownerId, name, userIds = []) {
        try {
            const { data: group, error: groupError } = await this.supabase
                .from('workspace_groups')
                .insert([{
                    name,
                    owner_id: ownerId.toString(),
                    created_at: new Date().toISOString()
                }])
                .select()
                .single();

            if (groupError) throw groupError;

            if (userIds && userIds.length > 0) {
                const memberRows = userIds.map(userId => ({
                    group_id: group.id,
                    user_id: userId.toString()
                }));
                const { error: memberError } = await this.supabase
                    .from('workspace_group_members')
                    .insert(memberRows);
                
                if (memberError) throw memberError;
            }

            return group;
        } catch (error) {
            console.error('Error creating workspace group:', error.message);
            throw error;
        }
    }

    async getWorkspaceGroups(ownerId) {
        try {
            const { data: groups, error: groupsError } = await this.supabase
                .from('workspace_groups')
                .select('*')
                .eq('owner_id', ownerId.toString())
                .order('created_at', { ascending: false });

            if (groupsError) throw groupsError;

            // Fetch members for each group
            const groupsWithMembers = await Promise.all((groups || []).map(async (group) => {
                const { data: members, error: membersError } = await this.supabase
                    .from('workspace_group_members')
                    .select('user_id')
                    .eq('group_id', group.id);
                
                const userIds = (members || []).map(m => m.user_id);
                let users = [];
                if (userIds.length > 0) {
                    const { data: usersData } = await this.supabase
                        .from('users')
                        .select('id, name, email')
                        .in('id', userIds);
                    users = usersData || [];
                }

                return {
                    ...group,
                    members: users
                };
            }));

            return groupsWithMembers;
        } catch (error) {
            console.error('Error fetching workspace groups:', error.message);
            throw error;
        }
    }

    async updateWorkspaceGroup(groupId, name, userIds = [], requestingUserId) {
        try {
            const { data: group, error: fetchError } = await this.supabase
                .from('workspace_groups')
                .select('owner_id')
                .eq('id', groupId)
                .single();

            if (fetchError) throw fetchError;
            if (group.owner_id.toString() !== requestingUserId.toString()) {
                throw new Error('Only the group owner can update it');
            }

            const { error: updateError } = await this.supabase
                .from('workspace_groups')
                .update({ name })
                .eq('id', groupId);

            if (updateError) throw updateError;

            // Replace members
            await this.supabase
                .from('workspace_group_members')
                .delete()
                .eq('group_id', groupId);

            if (userIds && userIds.length > 0) {
                const memberRows = userIds.map(userId => ({
                    group_id: groupId,
                    user_id: userId.toString()
                }));
                const { error: memberError } = await this.supabase
                    .from('workspace_group_members')
                    .insert(memberRows);
                
                if (memberError) throw memberError;
            }

            return true;
        } catch (error) {
            console.error('Error updating workspace group:', error.message);
            throw error;
        }
    }

    async deleteWorkspaceGroup(groupId, requestingUserId) {
        try {
            const { data: group, error: fetchError } = await this.supabase
                .from('workspace_groups')
                .select('owner_id')
                .eq('id', groupId)
                .single();

            if (fetchError) throw fetchError;
            if (group.owner_id.toString() !== requestingUserId.toString()) {
                throw new Error('Only the group owner can delete it');
            }

            const { error: deleteError } = await this.supabase
                .from('workspace_groups')
                .delete()
                .eq('id', groupId);

            if (deleteError) throw deleteError;
            return true;
        } catch (error) {
            console.error('Error deleting workspace group:', error.message);
            throw error;
        }
    }
    // ==================== Organization Management ====================

    async createOrganization(name) {
        try {
            const { data, error } = await this.supabase
                .from('organizations')
                .insert([{ name, created_at: new Date().toISOString() }])
                .select()
                .single();

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('Error creating organization:', error.message);
            throw error;
        }
    }

    async getOrganizations() {
        try {
            const { data, error } = await this.supabase
                .from('organizations')
                .select('*')
                .order('name', { ascending: true });

            if (error) throw error;
            return data || [];
        } catch (error) {
            console.error('Error fetching organizations:', error.message);
            throw error;
        }
    }

    async deleteOrganization(id) {
        try {
            // Unset organization_id for users in this org
            await this.supabase
                .from('users')
                .update({ organization_id: null, is_superuser: false })
                .eq('organization_id', id);

            const { error } = await this.supabase
                .from('organizations')
                .delete()
                .eq('id', id);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error deleting organization:', error.message);
            throw error;
        }
    }

    async updateUserOrganization(userId, organizationId) {
        try {
            const updateData = { organization_id: organizationId || null };
            // If removing from org, also remove superuser flag
            if (!organizationId) {
                updateData.is_superuser = false;
            }
            const { error } = await this.supabase
                .from('users')
                .update(updateData)
                .eq('id', userId);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error updating user organization:', error.message);
            throw error;
        }
    }

    async updateUserSuperuser(userId, isSuperuser) {
        try {
            const { error } = await this.supabase
                .from('users')
                .update({ is_superuser: isSuperuser })
                .eq('id', userId);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error updating user superuser status:', error.message);
            throw error;
        }
    }

    async getUsersByOrganization(orgId) {
        try {
            const { data, error } = await this.supabase
                .from('users')
                .select('id, name, email, role, is_superuser, organization_id')
                .eq('organization_id', orgId)
                .order('name', { ascending: true });

            if (error) throw error;
            return data || [];
        } catch (error) {
            console.error('Error fetching users by organization:', error.message);
            throw error;
        }
    }

    // ==================== Dashboard Access Sharing ====================

    async grantDashboardAccess(dashboardId, userId, accessLevel, grantedBy) {
        try {
            // Verify the granter is the dashboard owner or a CO_OWNER
            const { data: dashboard } = await this.supabase
                .from('dashboards')
                .select('user_id')
                .eq('id', dashboardId)
                .single();

            if (!dashboard) throw new Error('Dashboard not found');

            const isOwner = dashboard.user_id.toString() === grantedBy.toString();

            if (!isOwner) {
                // Check if granter is a CO_OWNER
                const { data: granterAccess } = await this.supabase
                    .from('dashboard_access')
                    .select('access_level')
                    .eq('dashboard_id', dashboardId)
                    .eq('user_id', grantedBy.toString())
                    .single();

                if (!granterAccess || granterAccess.access_level !== 'CO_OWNER') {
                    throw new Error('Only the dashboard owner or co-owners can share access');
                }
            }

            // Cannot grant access to the owner themselves
            if (dashboard.user_id.toString() === userId.toString()) {
                throw new Error('Cannot grant access to the dashboard owner');
            }

            // Upsert access
            const { data, error } = await this.supabase
                .from('dashboard_access')
                .upsert([{
                    dashboard_id: dashboardId,
                    user_id: userId,
                    access_level: accessLevel,
                    granted_by: grantedBy,
                    created_at: new Date().toISOString()
                }], { onConflict: 'dashboard_id,user_id' })
                .select()
                .single();

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('Error granting dashboard access:', error.message);
            throw error;
        }
    }

    async revokeDashboardAccess(dashboardId, userId, requestingUserId) {
        try {
            // Verify the requester is the owner or CO_OWNER
            const { data: dashboard } = await this.supabase
                .from('dashboards')
                .select('user_id')
                .eq('id', dashboardId)
                .single();

            if (!dashboard) throw new Error('Dashboard not found');

            const isOwner = dashboard.user_id.toString() === requestingUserId.toString();

            if (!isOwner) {
                const { data: requesterAccess } = await this.supabase
                    .from('dashboard_access')
                    .select('access_level')
                    .eq('dashboard_id', dashboardId)
                    .eq('user_id', requestingUserId.toString())
                    .single();

                if (!requesterAccess || requesterAccess.access_level !== 'CO_OWNER') {
                    throw new Error('Only the dashboard owner or co-owners can revoke access');
                }
            }

            const { error } = await this.supabase
                .from('dashboard_access')
                .delete()
                .eq('dashboard_id', dashboardId)
                .eq('user_id', userId);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error revoking dashboard access:', error.message);
            throw error;
        }
    }

    async getDashboardAccessList(dashboardId) {
        try {
            const { data, error } = await this.supabase
                .from('dashboard_access')
                .select('*, users!dashboard_access_user_id_fkey(id, name, email)')
                .eq('dashboard_id', dashboardId)
                .order('created_at', { ascending: true });

            if (error) throw error;

            return (data || []).map(entry => ({
                id: entry.id,
                dashboard_id: entry.dashboard_id,
                user_id: entry.user_id,
                user_name: entry.users?.name || 'Unknown',
                user_email: entry.users?.email || '',
                access_level: entry.access_level,
                granted_by: entry.granted_by,
                created_at: entry.created_at
            }));
        } catch (error) {
            console.error('Error fetching dashboard access list:', error.message);
            throw error;
        }
    }

    async getSharedDashboards(userId) {
        try {
            const { data, error } = await this.supabase
                .from('dashboard_access')
                .select('access_level, dashboards(*, users(id, name, email))')
                .eq('user_id', userId)
                .order('created_at', { ascending: false });

            if (error) throw error;

            return (data || []).filter(entry => entry.dashboards).map(entry => {
                const dashboard = entry.dashboards;
                const raw = dashboard.chart_configs;
                let chartConfigs, sections, filterColumns;

                if (raw && !Array.isArray(raw) && raw.charts) {
                    chartConfigs = raw.charts || [];
                    sections = raw.sections || [];
                    filterColumns = raw.filterColumns || [];
                } else {
                    chartConfigs = raw || [];
                    sections = [];
                    filterColumns = [];
                }

                return {
                    id: dashboard.id.toString(),
                    name: dashboard.name,
                    date: new Date(dashboard.created_at).toLocaleDateString(),
                    dataModel: dashboard.data_model || {},
                    chartConfigs,
                    sections,
                    filterColumns,
                    folder_id: dashboard.folder_id || null,
                    is_workspace: dashboard.is_workspace || false,
                    user_id: dashboard.user_id,
                    owner_name: dashboard.users?.name || 'Unknown',
                    shared_access_level: entry.access_level
                };
            });
        } catch (error) {
            console.error('Error fetching shared dashboards:', error.message);
            throw error;
        }
    }

    async grantDashboardAccess(dashboardId, userId, accessLevel, grantedBy) {
        try {
            // Check if grantedBy is owner or co-owner
            const { data: dashboard } = await this.supabase
                .from('dashboards')
                .select('user_id')
                .eq('id', dashboardId)
                .single();

            const isOwner = dashboard && dashboard.user_id.toString() === grantedBy.toString();
            
            if (!isOwner) {
                const { data: access } = await this.supabase
                    .from('dashboard_access')
                    .select('access_level')
                    .eq('dashboard_id', dashboardId)
                    .eq('user_id', grantedBy)
                    .single();

                if (!access || access.access_level !== 'CO_OWNER') {
                    throw new Error('Only owners or co-owners can grant access');
                }
            }

            const { data, error } = await this.supabase
                .from('dashboard_access')
                .upsert({
                    dashboard_id: dashboardId,
                    user_id: userId,
                    access_level: accessLevel,
                    granted_by: grantedBy,
                    created_at: new Date().toISOString()
                }, { onConflict: 'dashboard_id,user_id' })
                .select()
                .single();

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('Error granting dashboard access:', error.message);
            throw error;
        }
    }

    async revokeDashboardAccess(dashboardId, userId, revokerId) {
        try {
            // Check if revoker is owner or co-owner
            const { data: dashboard } = await this.supabase
                .from('dashboards')
                .select('user_id')
                .eq('id', dashboardId)
                .single();

            const isOwner = dashboard && dashboard.user_id.toString() === revokerId.toString();
            
            if (!isOwner) {
                const { data: access } = await this.supabase
                    .from('dashboard_access')
                    .select('access_level')
                    .eq('dashboard_id', dashboardId)
                    .eq('user_id', revokerId)
                    .single();

                if (!access || access.access_level !== 'CO_OWNER') {
                    throw new Error('Only owners or co-owners can revoke access');
                }
            }

            // Cannot revoke access from the owner themselves
            if (dashboard && dashboard.user_id.toString() === userId.toString()) {
                throw new Error('Cannot revoke access from the dashboard owner');
            }

            const { error } = await this.supabase
                .from('dashboard_access')
                .delete()
                .eq('dashboard_id', dashboardId)
                .eq('user_id', userId);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error revoking dashboard access:', error.message);
            throw error;
        }
    }

    // ==================== Scheduled Refresh ====================

    async createRefreshSchedule(dashboardId, userId, sourceType, sourceCredentials, refreshFrequency, refreshTimeUtc, refreshDay = null) {
        try {
            const { data, error } = await this.supabase
                .from('dashboard_refresh_schedules')
                .upsert([{
                    dashboard_id: dashboardId,
                    user_id: userId,
                    source_type: sourceType,
                    source_credentials: sourceCredentials,
                    refresh_frequency: refreshFrequency,
                    refresh_time_utc: refreshTimeUtc,
                    refresh_day: refreshDay,
                    is_active: true,
                    updated_at: new Date().toISOString()
                }], { onConflict: 'dashboard_id' })
                .select()
                .single();

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('Error creating refresh schedule:', error.message);
            throw error;
        }
    }

    async getRefreshSchedule(dashboardId) {
        try {
            const { data, error } = await this.supabase
                .from('dashboard_refresh_schedules')
                .select('*')
                .eq('dashboard_id', dashboardId)
                .maybeSingle();

            if (error) throw error;
            return data; // null if not found
        } catch (error) {
            console.error('Error fetching refresh schedule:', error.message);
            throw error;
        }
    }

    async updateRefreshSchedule(scheduleId, updates) {
        try {
            const { data, error } = await this.supabase
                .from('dashboard_refresh_schedules')
                .update({ ...updates, updated_at: new Date().toISOString() })
                .eq('id', scheduleId)
                .select()
                .single();

            if (error) throw error;
            return data;
        } catch (error) {
            console.error('Error updating refresh schedule:', error.message);
            throw error;
        }
    }

    async deleteRefreshSchedule(dashboardId) {
        try {
            const { error } = await this.supabase
                .from('dashboard_refresh_schedules')
                .delete()
                .eq('dashboard_id', dashboardId);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error deleting refresh schedule:', error.message);
            throw error;
        }
    }

    async getDueSchedules() {
        try {
            const { data, error } = await this.supabase
                .from('dashboard_refresh_schedules')
                .select('*, dashboards(id, name, data_model, user_id)')
                .eq('is_active', true)
                .or('last_refresh_status.is.null,last_refresh_status.neq.running');

            if (error) throw error;
            return data || [];
        } catch (error) {
            console.error('Error fetching due schedules:', error.message);
            return [];
        }
    }

    async updateLastRefresh(dashboardId, status, errorMsg = null) {
        try {
            const updateData = {
                last_refreshed_at: new Date().toISOString(),
                last_refresh_status: status,
                last_refresh_error: errorMsg,
                updated_at: new Date().toISOString()
            };

            const { error } = await this.supabase
                .from('dashboard_refresh_schedules')
                .update(updateData)
                .eq('dashboard_id', dashboardId);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error updating last refresh:', error.message);
            return false;
        }
    }

    async updateDashboardDataModel(dashboardId, dataModel) {
        try {
            const { error } = await this.supabase
                .from('dashboards')
                .update({ data_model: dataModel })
                .eq('id', dashboardId);

            if (error) throw error;
            return true;
        } catch (error) {
            console.error('Error updating dashboard data model:', error.message);
            throw error;
        }
    }
}

module.exports = new SupabaseService();
