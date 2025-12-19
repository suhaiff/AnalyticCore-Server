const { google } = require('googleapis');

/**
 * Service to handle Google Sheets API interactions using a Service Account.
 */
class GoogleSheetsService {
    constructor() {
        this.clientEmail = process.env.GOOGLE_SERVICE_ACCOUNT_EMAIL;
        // Handle private key with escaped newlines from environment variables
        this.privateKey = process.env.GOOGLE_PRIVATE_KEY ? process.env.GOOGLE_PRIVATE_KEY.replace(/\\n/g, '\n') : null;

        if (!this.clientEmail || !this.privateKey) {
            console.warn('Google Sheets API credentials missing (GOOGLE_SERVICE_ACCOUNT_EMAIL or GOOGLE_PRIVATE_KEY). Live import will not work.');
        }
    }

    /**
     * Creates an authenticated JWT client for Google APIs.
     */
    async getAuthClient() {
        if (!this.clientEmail || !this.privateKey) {
            throw new Error('Google Sheets API credentials not configured.');
        }

        const auth = new google.auth.GoogleAuth({
            credentials: {
                client_email: this.clientEmail,
                private_key: this.privateKey,
            },
            scopes: [
                'https://www.googleapis.com/auth/spreadsheets',
                'https://www.googleapis.com/auth/drive.readonly',
            ],
        });

        return await auth.getClient();
    }


    /**
     * Extracts the Spreadsheet ID from a Google Sheets URL.
     */
    extractSpreadsheetId(url) {
        if (!url) return null;
        const match = url.match(/\/spreadsheets\/d\/([a-zA-Z0-9-_]+)/);
        return match ? match[1] : null;
    }

    /**
     * Fetches metadata for a spreadsheet, including available sheet names.
     */
    async getMetadata(spreadsheetId) {
        try {
            const auth = await this.getAuthClient();
            const sheets = google.sheets({ version: 'v4', auth });

            const response = await sheets.spreadsheets.get({
                spreadsheetId,
            });

            return {
                title: response.data.properties.title,
                sheets: response.data.sheets.map(s => s.properties.title)
            };
        } catch (error) {
            console.error('Error fetching Google Sheet metadata:', error.message);
            if (error.code === 403) {
                throw new Error('Access denied. Please ensure the Google Sheet is shared with the service account email: ' + this.clientEmail);
            }
            if (error.code === 404) {
                throw new Error('Spreadsheet not found. Please check the URL.');
            }
            throw error;
        }
    }

    /**
     * Fetches raw values from a specific sheet and range.
     */
    async getSheetData(spreadsheetId, sheetName, range = 'A1:Z5000') {
        try {
            const auth = await this.getAuthClient();
            const sheets = google.sheets({ version: 'v4', auth });

            const response = await sheets.spreadsheets.values.get({
                spreadsheetId,
                range: `${sheetName}!${range}`,
            });

            // Returns data as array of arrays (rows)
            return response.data.values || [];
        } catch (error) {
            console.error('Error fetching Google Sheet data:', error.message);
            throw error;
        }
    }
}

module.exports = new GoogleSheetsService();
