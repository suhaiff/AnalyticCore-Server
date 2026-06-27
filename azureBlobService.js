const { BlobServiceClient } = require('@azure/storage-blob');

const azureBlobService = {
    /**
     * Creates a BlobServiceClient instance
     */
    getClient: (connectionString) => {
        return BlobServiceClient.fromConnectionString(connectionString);
    },

    /**
     * Test the connection to an Azure Blob container
     */
    testConnection: async (connectionString, containerName) => {
        try {
            const blobServiceClient = azureBlobService.getClient(connectionString);
            const containerClient = blobServiceClient.getContainerClient(containerName);
            
            // Attempt to get container properties to test access
            await containerClient.getProperties();
            return { success: true };
        } catch (error) {
            console.error('Azure testConnection error:', error);
            return { success: false, error: error.message };
        }
    },

    /**
     * List files in the Azure Blob container (filtered by .csv, .xls, .xlsx)
     */
    listFiles: async (connectionString, containerName) => {
        try {
            const blobServiceClient = azureBlobService.getClient(connectionString);
            const containerClient = blobServiceClient.getContainerClient(containerName);
            
            const files = [];
            for await (const blob of containerClient.listBlobsFlat()) {
                const name = blob.name.toLowerCase();
                if (name.endsWith('.csv') || name.endsWith('.xlsx') || name.endsWith('.xls')) {
                    files.push({
                        id: blob.name,
                        name: blob.name.split('/').pop(),
                        size: blob.properties.contentLength,
                        lastModified: blob.properties.lastModified
                    });
                }
            }

            return { success: true, files };
        } catch (error) {
            console.error('Azure listFiles error:', error);
            return { success: false, error: error.message };
        }
    },

    /**
     * Fetch a specific file as a Buffer
     */
    fetchFile: async (connectionString, containerName, blobName) => {
        try {
            const blobServiceClient = azureBlobService.getClient(connectionString);
            const containerClient = blobServiceClient.getContainerClient(containerName);
            const blobClient = containerClient.getBlobClient(blobName);
            
            const downloadBlockBlobResponse = await blobClient.download();
            
            const streamToBuffer = (readableStream) => {
                return new Promise((resolve, reject) => {
                    const chunks = [];
                    readableStream.on("data", (data) => {
                        chunks.push(data instanceof Buffer ? data : Buffer.from(data));
                    });
                    readableStream.on("end", () => {
                        resolve(Buffer.concat(chunks));
                    });
                    readableStream.on("error", reject);
                });
            };

            const buffer = await streamToBuffer(downloadBlockBlobResponse.readableStreamBody);
            
            return { success: true, buffer, mimeType: downloadBlockBlobResponse.contentType };
        } catch (error) {
            console.error('Azure fetchFile error:', error);
            return { success: false, error: error.message };
        }
    }
};

module.exports = azureBlobService;
