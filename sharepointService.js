const axios = require('axios');
const crypto = require('crypto');

/**
 * Service to handle SharePoint interactions via Microsoft Graph API
 * Uses OAuth 2.0 Client Credentials flow (service account, no user login)
 */
class SharePointService {
    constructor() {
        this.tenantId = process.env.SHAREPOINT_TENANT_ID;
        this.clientId = process.env.SHAREPOINT_CLIENT_ID;
        this.clientSecret = process.env.SHAREPOINT_CLIENT_SECRET;
        this.encryptionKey = process.env.SHAREPOINT_ENCRYPTION_KEY;

        // Token cache (in-memory for now, should be DB in production)
        this.tokenCache = {
            accessToken: null,
            expiresAt: null
        };

        // Validate configuration
        if (!this.tenantId || !this.clientId || !this.clientSecret) {
            console.warn('SharePoint API credentials missing. SharePoint import will not work.');
            console.warn('Required env vars: SHAREPOINT_TENANT_ID, SHAREPOINT_CLIENT_ID, SHAREPOINT_CLIENT_SECRET');
        }

        if (!this.encryptionKey || this.encryptionKey.length !== 32) {
            console.warn('SHAREPOINT_ENCRYPTION_KEY must be exactly 32 characters for AES-256 encryption');
        }
    }

    /**
     * Checks if SharePoint is properly configured
     */
    isConfigured() {
        return !!(this.tenantId && this.clientId && this.clientSecret && this.encryptionKey);
    }

    /**
     * Encrypts sensitive data (tokens) using AES-256-CBC
     */
    encrypt(text) {
        if (!this.encryptionKey) throw new Error('Encryption key not configured');

        const iv = crypto.randomBytes(16);
        const cipher = crypto.createCipheriv('aes-256-cbc', Buffer.from(this.encryptionKey), iv);

        let encrypted = cipher.update(text, 'utf8', 'hex');
        encrypted += cipher.final('hex');

        return iv.toString('hex') + ':' + encrypted;
    }

    /**
     * Decrypts encrypted tokens
     */
    decrypt(encryptedText) {
        if (!this.encryptionKey) throw new Error('Encryption key not configured');

        const parts = encryptedText.split(':');
        const iv = Buffer.from(parts[0], 'hex');
        const encrypted = parts[1];

        const decipher = crypto.createDecipheriv('aes-256-cbc', Buffer.from(this.encryptionKey), iv);

        let decrypted = decipher.update(encrypted, 'hex', 'utf8');
        decrypted += decipher.final('utf8');

        return decrypted;
    }

    /**
     * Acquires an access token using Client Credentials flow
     */
    async getAccessToken() {
        if (!this.isConfigured()) {
            throw new Error('SharePoint is not properly configured. Please check environment variables.');
        }

        // Check if we have a valid cached token
        if (this.tokenCache.accessToken && this.tokenCache.expiresAt > Date.now()) {
            console.log('Using cached SharePoint access token');
            return this.tokenCache.accessToken;
        }

        console.log('Acquiring new SharePoint access token...');

        try {
            const tokenEndpoint = `https://login.microsoftonline.com/${this.tenantId}/oauth2/v2.0/token`;

            const params = new URLSearchParams({
                client_id: this.clientId,
                client_secret: this.clientSecret,
                scope: 'https://graph.microsoft.com/.default',
                grant_type: 'client_credentials'
            });

            const response = await axios.post(tokenEndpoint, params, {
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            });

            const { access_token, expires_in } = response.data;

            // Cache the token (expires_in is in seconds, subtract 5 minutes for safety)
            this.tokenCache.accessToken = access_token;
            this.tokenCache.expiresAt = Date.now() + ((expires_in - 300) * 1000);

            console.log('SharePoint access token acquired successfully');
            return access_token;

        } catch (error) {
            console.error('Error acquiring SharePoint access token:', error.response?.data || error.message);
            throw new Error('Failed to authenticate with SharePoint: ' + (error.response?.data?.error_description || error.message));
        }
    }

    /**
     * Makes an authenticated request to Microsoft Graph API
     */
    async graphRequest(endpoint, method = 'GET', data = null) {
        const accessToken = await this.getAccessToken();

        const config = {
            method,
            url: `https://graph.microsoft.com/v1.0${endpoint}`,
            headers: {
                'Authorization': `Bearer ${accessToken}`,
                'Content-Type': 'application/json'
            }
        };

        if (data) {
            config.data = data;
        }

        try {
            const response = await axios(config);
            return response.data;
        } catch (error) {
            console.error(`Graph API error (${method} ${endpoint}):`, error.response?.data || error.message);
            throw new Error(error.response?.data?.error?.message || error.message);
        }
    }

    /**
     * Gets all SharePoint sites accessible to the app
     */
    async getSites() {
        try {
            const data = await this.graphRequest('/sites?search=*');
            return data.value.map(site => ({
                id: site.id,
                name: site.displayName || site.name,
                webUrl: site.webUrl,
                description: site.description || ''
            }));
        } catch (error) {
            console.error('Error fetching SharePoint sites:', error.message);
            throw error;
        }
    }

    /**
     * Gets a specific site by URL or ID
     */
    async getSite(siteIdOrUrl) {
        try {
            // If it looks like a URL, use the sites endpoint with hostname and path
            if (siteIdOrUrl.startsWith('http')) {
                const url = new URL(siteIdOrUrl);
                const hostname = url.hostname;
                const pathname = url.pathname || '/';
                const endpoint = `/sites/${hostname}:${pathname}`;
                return await this.graphRequest(endpoint);
            } else {
                // Otherwise treat as site ID
                return await this.graphRequest(`/sites/${siteIdOrUrl}`);
            }
        } catch (error) {
            console.error('Error fetching SharePoint site:', error.message);
            throw error;
        }
    }

    /**
     * Gets all lists in a SharePoint site
     */
    async getLists(siteId) {
        try {
            const data = await this.graphRequest(`/sites/${siteId}/lists`);
            return data.value.map(list => ({
                id: list.id,
                name: list.displayName || list.name,
                description: list.description || '',
                itemCount: list.list?.contentTypesEnabled ? 0 : (list.list?.itemCount || 0),
                webUrl: list.webUrl,
                listType: list.list?.template || 'genericList'
            }));
        } catch (error) {
            console.error('Error fetching SharePoint lists:', error.message);
            throw error;
        }
    }

    /**
     * Gets columns (fields) for a SharePoint list
     */
    async getListColumns(siteId, listId) {
        try {
            const data = await this.graphRequest(`/sites/${siteId}/lists/${listId}/columns`);
            return data.value
                .filter(col => !col.hidden && !col.readOnly) // Filter out system columns
                .map(col => ({
                    name: col.name,
                    displayName: col.displayName,
                    type: col.columnType || col.type,
                    required: col.required || false
                }));
        } catch (error) {
            console.error('Error fetching SharePoint list columns:', error.message);
            throw error;
        }
    }

    /**
     * Gets all items from a SharePoint list with pagination support
     */
    async getListItems(siteId, listId, options = {}) {
        try {
            const { top = 5000, select = null, filter = null } = options;

            let endpoint = `/sites/${siteId}/lists/${listId}/items?expand=fields&$top=${top}`;

            if (select) {
                endpoint += `&$select=${select}`;
            }

            if (filter) {
                endpoint += `&$filter=${encodeURIComponent(filter)}`;
            }

            let allItems = [];
            let nextLink = endpoint;

            // Handle pagination
            while (nextLink) {
                const data = nextLink.startsWith('http')
                    ? (await axios.get(nextLink, {
                        headers: { 'Authorization': `Bearer ${await this.getAccessToken()}` }
                    })).data
                    : await this.graphRequest(nextLink);

                allItems = allItems.concat(data.value || []);
                nextLink = data['@odata.nextLink'] || null;

                console.log(`Fetched ${allItems.length} items so far...`);

                // Safety limit to prevent infinite loops
                if (allItems.length >= 100000) {
                    console.warn('Reached 100k item limit, stopping pagination');
                    break;
                }
            }

            console.log(`Total items fetched: ${allItems.length}`);
            return allItems;

        } catch (error) {
            console.error('Error fetching SharePoint list items:', error.message);
            throw error;
        }
    }

    /**
     * Converts SharePoint list items to normalized array of arrays format
     * Compatible with existing Excel/Google Sheets data structure
     */
    normalizeListData(items, columns) {
        if (!items || items.length === 0) return [];

        // Extract column names
        const columnNames = columns.map(col => col.name);

        // Create header row
        const headers = columns.map(col => col.displayName || col.name);

        // Convert items to rows
        const rows = items.map(item => {
            const fields = item.fields || {};
            return columnNames.map(colName => {
                const value = fields[colName];

                // Handle different SharePoint field types
                if (value === null || value === undefined) {
                    return '';
                }

                // Handle lookup fields
                if (typeof value === 'object' && value.LookupValue) {
                    return value.LookupValue;
                }

                // Handle person/group fields
                if (typeof value === 'object' && value.Email) {
                    return value.Email;
                }

                // Handle arrays (multi-select fields)
                if (Array.isArray(value)) {
                    return value.join('; ');
                }

                // Handle dates
                if (typeof value === 'string' && value.match(/^\d{4}-\d{2}-\d{2}T/)) {
                    return new Date(value).toLocaleString();
                }

                return String(value);
            });
        });

        // Return with headers as first row
        return [headers, ...rows];
    }

    /**
     * Main import function - fetches and normalizes SharePoint data
     * Uses service account (for backward compatibility)
     */
    async importList(siteId, listId, options = {}) {
        try {
            console.log(`Importing SharePoint list: ${listId} from site: ${siteId}`);

            // 1. Get list metadata (columns)
            const columns = await this.getListColumns(siteId, listId);
            console.log(`Found ${columns.length} columns`);

            // 2. Get list items
            const items = await this.getListItems(siteId, listId, options);
            console.log(`Found ${items.length} items`);

            // 3. Normalize to array of arrays
            const normalizedData = this.normalizeListData(items, columns);

            return {
                columns,
                items,
                data: normalizedData,
                rowCount: normalizedData.length,
                columnCount: columns.length
            };

        } catch (error) {
            console.error('Error importing SharePoint list:', error.message);
            throw error;
        }
    }

    // ============================================
    // PER-USER OAUTH METHODS (Delegated Permissions)
    // ============================================

    /**
     * Makes an authenticated request to Microsoft Graph API using user's access token
     */
    async graphRequestWithUserToken(userAccessToken, endpoint, method = 'GET', data = null) {
        const config = {
            method,
            url: `https://graph.microsoft.com/v1.0${endpoint}`,
            headers: {
                'Authorization': `Bearer ${userAccessToken}`,
                'Content-Type': 'application/json'
            }
        };

        if (data) {
            config.data = data;
        }

        try {
            const response = await axios(config);
            return response.data;
        } catch (error) {
            console.error(`Graph API error (${method} ${endpoint}):`, error.response?.data || error.message);
            throw new Error(error.response?.data?.error?.message || error.message);
        }
    }

    /**
     * Gets all SharePoint sites accessible to the user (per-user OAuth)
     */
    async getUserSites(userAccessToken) {
        try {
            const data = await this.graphRequestWithUserToken(userAccessToken, '/sites?search=*');
            return data.value.map(site => ({
                id: site.id,
                name: site.displayName || site.name,
                webUrl: site.webUrl,
                description: site.description || ''
            }));
        } catch (error) {
            console.error('Error fetching user SharePoint sites:', error.message);
            throw error;
        }
    }

    /**
     * Gets all lists in a SharePoint site (per-user OAuth)
     */
    async getUserLists(userAccessToken, siteId) {
        try {
            const data = await this.graphRequestWithUserToken(userAccessToken, `/sites/${siteId}/lists`);
            return data.value.map(list => ({
                id: list.id,
                name: list.displayName || list.name,
                description: list.description || '',
                itemCount: list.list?.contentTypesEnabled ? 0 : (list.list?.itemCount || 0),
                webUrl: list.webUrl,
                listType: list.list?.template || 'genericList'
            }));
        } catch (error) {
            console.error('Error fetching user SharePoint lists:', error.message);
            throw error;
        }
    }

    /**
     * Gets columns (fields) for a SharePoint list (per-user OAuth)
     */
    async getUserListColumns(userAccessToken, siteId, listId) {
        try {
            const data = await this.graphRequestWithUserToken(userAccessToken, `/sites/${siteId}/lists/${listId}/columns`);
            return data.value
                .filter(col => !col.hidden && !col.readOnly) // Filter out system columns
                .map(col => ({
                    name: col.name,
                    displayName: col.displayName,
                    type: col.columnType || col.type,
                    required: col.required || false
                }));
        } catch (error) {
            console.error('Error fetching user SharePoint list columns:', error.message);
            throw error;
        }
    }

    /**
     * Gets all items from a SharePoint list with pagination support (per-user OAuth)
     */
    async getUserListItems(userAccessToken, siteId, listId, options = {}) {
        try {
            const { top = 5000, select = null, filter = null } = options;

            let endpoint = `/sites/${siteId}/lists/${listId}/items?expand=fields&$top=${top}`;

            if (select) {
                endpoint += `&$select=${select}`;
            }

            if (filter) {
                endpoint += `&$filter=${encodeURIComponent(filter)}`;
            }

            let allItems = [];
            let nextLink = endpoint;

            // Handle pagination
            while (nextLink) {
                const data = nextLink.startsWith('http')
                    ? (await axios.get(nextLink, {
                        headers: { 'Authorization': `Bearer ${userAccessToken}` }
                    })).data
                    : await this.graphRequestWithUserToken(userAccessToken, nextLink);

                allItems = allItems.concat(data.value || []);
                nextLink = data['@odata.nextLink'] || null;

                console.log(`Fetched ${allItems.length} items so far...`);

                // Safety limit to prevent infinite loops
                if (allItems.length >= 100000) {
                    console.warn('Reached 100k item limit, stopping pagination');
                    break;
                }
            }

            console.log(`Total items fetched: ${allItems.length}`);
            return allItems;

        } catch (error) {
            console.error('Error fetching user SharePoint list items:', error.message);
            throw error;
        }
    }

    /**
     * Main import function - fetches and normalizes SharePoint data (per-user OAuth)
     */
    async importUserList(userAccessToken, siteId, listId, options = {}) {
        try {
            console.log(`Importing user SharePoint list: ${listId} from site: ${siteId}`);

            // 1. Get list metadata (columns)
            const columns = await this.getUserListColumns(userAccessToken, siteId, listId);
            console.log(`Found ${columns.length} columns`);

            // 2. Get list items
            const items = await this.getUserListItems(userAccessToken, siteId, listId, options);
            console.log(`Found ${items.length} items`);

            // 3. Normalize to array of arrays
            const normalizedData = this.normalizeListData(items, columns);

            return {
                columns,
                items,
                data: normalizedData,
                rowCount: normalizedData.length,
                columnCount: columns.length
            };

        } catch (error) {
            console.error('Error importing user SharePoint list:', error.message);
            throw error;
        }
    }
}

module.exports = new SharePointService();
