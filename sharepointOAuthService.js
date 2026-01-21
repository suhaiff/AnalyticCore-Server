const axios = require('axios');
const crypto = require('crypto');
const supabaseService = require('./supabaseService');

/**
 * Service to handle SharePoint OAuth 2.0 (Delegated Permissions - Per User)
 * This implements the Authorization Code Flow for multi-tenant SaaS
 */
class SharePointOAuthService {
    constructor() {
        // OAuth Configuration (Delegated - per user)
        this.clientId = process.env.SHAREPOINT_OAUTH_CLIENT_ID || process.env.SHAREPOINT_CLIENT_ID;
        this.clientSecret = process.env.SHAREPOINT_OAUTH_CLIENT_SECRET || process.env.SHAREPOINT_CLIENT_SECRET;
        this.tenantId = process.env.SHAREPOINT_OAUTH_TENANT_ID || process.env.SHAREPOINT_TENANT_ID || 'common';
        this.redirectUri = process.env.SHAREPOINT_REDIRECT_URI;
        this.encryptionKey = process.env.SHAREPOINT_ENCRYPTION_KEY;

        // Validate configuration
        if (!this.clientId || !this.clientSecret) {
            console.warn('SharePoint OAuth credentials missing. Per-user SharePoint import will not work.');
            console.warn('Required env vars: SHAREPOINT_OAUTH_CLIENT_ID, SHAREPOINT_OAUTH_CLIENT_SECRET');
        }

        if (!this.encryptionKey || this.encryptionKey.length !== 32) {
            console.warn('SHAREPOINT_ENCRYPTION_KEY must be exactly 32 characters for AES-256 encryption');
        }

        if (!this.redirectUri) {
            console.warn('SHAREPOINT_REDIRECT_URI not set. Example: https://your-backend.com/auth/sharepoint/callback');
        }
    }

    /**
     * Checks if OAuth is properly configured
     */
    isConfigured() {
        return !!(
            this.clientId &&
            this.clientSecret &&
            this.encryptionKey &&
            this.redirectUri &&
            this.encryptionKey.length === 32
        );
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
     * Generates the OAuth authorization URL for the user to visit
     */
    getAuthorizationUrl(userId, state = null) {
        if (!this.isConfigured()) {
            throw new Error('SharePoint OAuth is not properly configured');
        }

        // Use 'common' for multitenant or specific tenant ID
        const authEndpoint = `https://login.microsoftonline.com/${this.tenantId}/oauth2/v2.0/authorize`;

        // Encode state with userId for security
        const stateParam = state || Buffer.from(JSON.stringify({ userId, timestamp: Date.now() })).toString('base64');

        const params = new URLSearchParams({
            client_id: this.clientId,
            response_type: 'code',
            redirect_uri: this.redirectUri,
            response_mode: 'query',
            scope: 'User.Read Sites.Read.All offline_access',  // Delegated permissions
            state: stateParam
        });

        return `${authEndpoint}?${params.toString()}`;
    }

    /**
     * Exchanges authorization code for access and refresh tokens
     */
    async exchangeCodeForTokens(code) {
        if (!this.isConfigured()) {
            throw new Error('SharePoint OAuth is not properly configured');
        }

        const tokenEndpoint = `https://login.microsoftonline.com/${this.tenantId}/oauth2/v2.0/token`;

        const params = new URLSearchParams({
            client_id: this.clientId,
            client_secret: this.clientSecret,
            code: code,
            redirect_uri: this.redirectUri,
            grant_type: 'authorization_code',
            scope: 'User.Read Sites.Read.All offline_access'
        });

        try {
            const response = await axios.post(tokenEndpoint, params, {
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            });

            const { access_token, refresh_token, expires_in } = response.data;

            return {
                accessToken: access_token,
                refreshToken: refresh_token,
                expiresIn: expires_in
            };
        } catch (error) {
            console.error('Error exchanging code for tokens:', error.response?.data || error.message);
            throw new Error('Failed to obtain SharePoint access token: ' + (error.response?.data?.error_description || error.message));
        }
    }

    /**
     * Refreshes an expired access token using the refresh token
     */
    async refreshAccessToken(refreshToken) {
        if (!this.isConfigured()) {
            throw new Error('SharePoint OAuth is not properly configured');
        }

        const tokenEndpoint = `https://login.microsoftonline.com/${this.tenantId}/oauth2/v2.0/token`;

        const params = new URLSearchParams({
            client_id: this.clientId,
            client_secret: this.clientSecret,
            refresh_token: refreshToken,
            grant_type: 'refresh_token',
            scope: 'User.Read Sites.Read.All offline_access'
        });

        try {
            const response = await axios.post(tokenEndpoint, params, {
                headers: {
                    'Content-Type': 'application/x-www-form-urlencoded'
                }
            });

            const { access_token, refresh_token, expires_in } = response.data;

            return {
                accessToken: access_token,
                refreshToken: refresh_token || refreshToken,  // Sometimes refresh token is not returned
                expiresIn: expires_in
            };
        } catch (error) {
            console.error('Error refreshing token:', error.response?.data || error.message);
            throw new Error('Failed to refresh SharePoint token: ' + (error.response?.data?.error_description || error.message));
        }
    }

    /**
     * Stores OAuth tokens for a user in the database
     */
    async storeUserTokens(userId, accessToken, refreshToken, expiresIn, tenantId = null) {
        const encryptedAccessToken = this.encrypt(accessToken);
        const encryptedRefreshToken = this.encrypt(refreshToken);
        const expiresAt = new Date(Date.now() + (expiresIn * 1000)).toISOString();

        try {
            // Check if user already has a connection
            const { data: existing } = await supabaseService.supabase
                .from('sharepoint_connections')
                .select('*')
                .eq('user_id', userId)
                .single();

            if (existing) {
                // Update existing connection
                const { data, error } = await supabaseService.supabase
                    .from('sharepoint_connections')
                    .update({
                        access_token: encryptedAccessToken,
                        refresh_token: encryptedRefreshToken,
                        expires_at: expiresAt,
                        tenant_id: tenantId,
                        updated_at: new Date().toISOString()
                    })
                    .eq('user_id', userId)
                    .select()
                    .single();

                if (error) throw error;
                return data;
            } else {
                // Create new connection
                const { data, error } = await supabaseService.supabase
                    .from('sharepoint_connections')
                    .insert({
                        user_id: userId,
                        access_token: encryptedAccessToken,
                        refresh_token: encryptedRefreshToken,
                        expires_at: expiresAt,
                        tenant_id: tenantId
                    })
                    .select()
                    .single();

                if (error) throw error;
                return data;
            }
        } catch (error) {
            console.error('Error storing SharePoint tokens:', error);
            throw new Error('Failed to store SharePoint connection: ' + error.message);
        }
    }

    /**
     * Gets the access token for a user, refreshing if necessary
     */
    async getUserAccessToken(userId) {
        try {
            // Get user's connection from database
            const { data: connection, error } = await supabaseService.supabase
                .from('sharepoint_connections')
                .select('*')
                .eq('user_id', userId)
                .single();

            if (error || !connection) {
                throw new Error('User has not connected their SharePoint account');
            }

            // Check if token is expired
            const expiresAt = new Date(connection.expires_at);
            const now = new Date();

            // Refresh if expired or expiring within 5 minutes
            if (expiresAt <= new Date(now.getTime() + 5 * 60 * 1000)) {
                console.log(`SharePoint token for user ${userId} is expired or expiring soon, refreshing...`);

                const decryptedRefreshToken = this.decrypt(connection.refresh_token);
                const { accessToken, refreshToken, expiresIn } = await this.refreshAccessToken(decryptedRefreshToken);

                // Store updated tokens
                await this.storeUserTokens(userId, accessToken, refreshToken, expiresIn, connection.tenant_id);

                return accessToken;
            }

            // Token is still valid
            return this.decrypt(connection.access_token);

        } catch (error) {
            console.error(`Error getting access token for user ${userId}:`, error.message);
            throw error;
        }
    }

    /**
     * Checks if a user has connected their SharePoint account
     */
    async isUserConnected(userId) {
        try {
            const { data, error } = await supabaseService.supabase
                .from('sharepoint_connections')
                .select('id')
                .eq('user_id', userId)
                .single();

            return !error && !!data;
        } catch (error) {
            return false;
        }
    }

    /**
     * Disconnects a user's SharePoint account
     */
    async disconnectUser(userId) {
        try {
            const { error } = await supabaseService.supabase
                .from('sharepoint_connections')
                .delete()
                .eq('user_id', userId);

            if (error) throw error;

            console.log(`SharePoint connection removed for user ${userId}`);
            return true;
        } catch (error) {
            console.error('Error disconnecting SharePoint:', error);
            throw new Error('Failed to disconnect SharePoint: ' + error.message);
        }
    }

    /**
     * Makes an authenticated request to Microsoft Graph API using user's token
     */
    async graphRequest(userId, endpoint, method = 'GET', data = null) {
        const accessToken = await this.getUserAccessToken(userId);

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

            // If unauthorized, the token might be invalid - disconnect user
            if (error.response?.status === 401) {
                console.warn(`Invalid token for user ${userId}, disconnecting...`);
                await this.disconnectUser(userId);
            }

            throw new Error(error.response?.data?.error?.message || error.message);
        }
    }
}

module.exports = new SharePointOAuthService();
