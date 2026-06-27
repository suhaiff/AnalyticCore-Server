const { Storage } = require('@google-cloud/storage');

const gcsService = {
    /**
     * Creates a GCS Storage instance
     */
    getClient: (credentialsJson) => {
        let creds;
        if (typeof credentialsJson === 'string') {
            creds = JSON.parse(credentialsJson);
        } else {
            creds = credentialsJson;
        }
        
        return new Storage({
            projectId: creds.project_id,
            credentials: creds
        });
    },

    /**
     * Test the connection to a GCS bucket
     */
    testConnection: async (credentialsJson, bucketName) => {
        try {
            const storage = gcsService.getClient(credentialsJson);
            const bucket = storage.bucket(bucketName);
            
            // Check if bucket exists
            const [exists] = await bucket.exists();
            if (!exists) {
                return { success: false, error: `Bucket ${bucketName} does not exist.` };
            }
            
            // Test access by fetching metadata
            await bucket.getMetadata();
            return { success: true };
        } catch (error) {
            console.error('GCS testConnection error:', error);
            return { success: false, error: error.message };
        }
    },

    /**
     * List files in the GCS bucket (filtered by .csv, .xls, .xlsx)
     */
    listFiles: async (credentialsJson, bucketName) => {
        try {
            const storage = gcsService.getClient(credentialsJson);
            const bucket = storage.bucket(bucketName);
            
            const [files] = await bucket.getFiles();
            
            const filteredFiles = files.filter(file => {
                const name = file.name.toLowerCase();
                return name.endsWith('.csv') || name.endsWith('.xlsx') || name.endsWith('.xls');
            }).map(file => ({
                id: file.name,
                name: file.name.split('/').pop(),
                size: file.metadata.size,
                lastModified: file.metadata.updated
            }));

            return { success: true, files: filteredFiles };
        } catch (error) {
            console.error('GCS listFiles error:', error);
            return { success: false, error: error.message };
        }
    },

    /**
     * Fetch a specific file as a Buffer
     */
    fetchFile: async (credentialsJson, bucketName, fileName) => {
        try {
            const storage = gcsService.getClient(credentialsJson);
            const bucket = storage.bucket(bucketName);
            const file = bucket.file(fileName);
            
            const [buffer] = await file.download();
            const [metadata] = await file.getMetadata();
            
            return { success: true, buffer, mimeType: metadata.contentType };
        } catch (error) {
            console.error('GCS fetchFile error:', error);
            return { success: false, error: error.message };
        }
    }
};

module.exports = gcsService;
