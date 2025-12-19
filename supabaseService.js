const { createClient } = require('@supabase/supabase-js');

class SupabaseService {
    constructor() {
        // IMPORTANT: All credentials MUST be stored in .env file
        // NEVER hardcode credentials in source code!
        this.supabaseUrl = process.env.SUPABASE_URL;
        this.supabaseKey = process.env.SUPABASE_KEY;

        // Validate that all required environment variables are present
        if (!this.supabaseUrl) {
            throw new Error('Missing required environment variable: SUPABASE_URL');
        }
        if (!this.supabaseKey) {
            throw new Error('Missing required environment variable: SUPABASE_KEY');
        }

        // Initialize Supabase client
        this.supabase = createClient(this.supabaseUrl, this.supabaseKey);
    }

    // ==================== User Management ====================

    async getUsers() {
        try {
            const { data, error } = await this.supabase
                .from('users')
                .select('*')
                .order('created_at', { ascending: false });

            if (error) throw error;
            return data || [];
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

    async createUser(name, email, password, role = 'USER') {
        try {
            const { data, error } = await this.supabase
                .from('users')
                .insert([
                    {
                        name,
                        email,
                        password, // In production, use bcrypt to hash passwords!
                        role,
                        created_at: new Date().toISOString()
                    }
                ])
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

    // ==================== Dashboard Management ====================

    async createDashboard(userId, name, dataModel, chartConfigs) {
        try {
            console.log('Creating dashboard with params:', {
                userId,
                name,
                dataModelType: typeof dataModel,
                chartConfigsType: typeof chartConfigs
            });

            const { data, error } = await this.supabase
                .from('dashboards')
                .insert([
                    {
                        user_id: userId,
                        name,
                        data_model: dataModel,
                        chart_configs: chartConfigs,
                        created_at: new Date().toISOString()
                    }
                ])
                .select()
                .single();

            if (error) throw error;

            console.log('Dashboard created successfully:', data);
            return {
                id: data.id,
                message: 'Dashboard saved'
            };
        } catch (error) {
            console.error('Error creating dashboard:', error.message);
            throw error;
        }
    }

    async getDashboardsByUser(userId) {
        try {
            const { data, error } = await this.supabase
                .from('dashboards')
                .select('*')
                .eq('user_id', userId)
                .order('created_at', { ascending: false });

            if (error) throw error;

            return (data || []).map(dashboard => ({
                id: dashboard.id.toString(),
                name: dashboard.name,
                date: new Date(dashboard.created_at).toLocaleDateString(),
                dataModel: dashboard.data_model || {},
                chartConfigs: dashboard.chart_configs || []
            }));
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

            return (data || []).map(dashboard => ({
                id: dashboard.id.toString(),
                name: dashboard.name,
                user_id: dashboard.user_id,
                user_name: dashboard.users?.name || 'Unknown',
                user_email: dashboard.users?.email || '',
                date: new Date(dashboard.created_at).toLocaleDateString(),
                dataModel: dashboard.data_model || {},
                chartConfigs: dashboard.chart_configs || []
            }));
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

            if (error) throw error;
        } catch (error) {
            console.error('Error creating upload log:', error.message);
            throw error;
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

                const sheetData = (excelData || []).map(row => row.row_data);

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

            if (error) throw error;
        } catch (error) {
            console.error('Error creating data config log:', error.message);
            throw error;
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
}

module.exports = new SupabaseService();
