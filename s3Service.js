const { S3Client, ListObjectsV2Command, GetObjectCommand } = require('@aws-sdk/client-s3');
const { Readable } = require('stream');

/**
 * Creates an S3 Client instance
 */
const getS3Client = (credentials) => {
    return new S3Client({
        region: credentials.region,
        credentials: {
            accessKeyId: credentials.accessKeyId,
            secretAccessKey: credentials.secretAccessKey
        }
    });
};

const s3Service = {
    /**
     * Test the connection to an S3 bucket
     */
    testConnection: async (credentials, bucket) => {
        try {
            const client = getS3Client(credentials);
            const command = new ListObjectsV2Command({ Bucket: bucket, MaxKeys: 1 });
            await client.send(command);
            return { success: true };
        } catch (error) {
            console.error('S3 testConnection error:', error);
            return { success: false, error: error.message };
        }
    },

    /**
     * List files in the S3 bucket (filtered by .csv, .xls, .xlsx)
     */
    listFiles: async (credentials, bucket) => {
        try {
            const client = getS3Client(credentials);
            let isTruncated = true;
            let continuationToken = undefined;
            const files = [];

            while (isTruncated) {
                const command = new ListObjectsV2Command({
                    Bucket: bucket,
                    ContinuationToken: continuationToken
                });
                
                const response = await client.send(command);
                
                if (response.Contents) {
                    for (const item of response.Contents) {
                        const key = item.Key;
                        const lowerKey = key.toLowerCase();
                        if (lowerKey.endsWith('.csv') || lowerKey.endsWith('.xlsx') || lowerKey.endsWith('.xls')) {
                            files.push({
                                id: key,
                                name: key.split('/').pop(),
                                size: item.Size,
                                lastModified: item.LastModified
                            });
                        }
                    }
                }
                
                isTruncated = response.IsTruncated;
                continuationToken = response.NextContinuationToken;
            }

            return { success: true, files };
        } catch (error) {
            console.error('S3 listFiles error:', error);
            return { success: false, error: error.message };
        }
    },

    /**
     * Fetch a specific file as a Buffer
     */
    fetchFile: async (credentials, bucket, fileKey) => {
        try {
            const client = getS3Client(credentials);
            const command = new GetObjectCommand({ Bucket: bucket, Key: fileKey });
            const response = await client.send(command);
            
            // Convert ReadableStream to Buffer
            const streamToBuffer = (stream) =>
                new Promise((resolve, reject) => {
                    const chunks = [];
                    stream.on("data", (chunk) => chunks.push(chunk));
                    stream.on("error", reject);
                    stream.on("end", () => resolve(Buffer.concat(chunks)));
                });

            const buffer = await streamToBuffer(response.Body);
            
            return { success: true, buffer, mimeType: response.ContentType };
        } catch (error) {
            console.error('S3 fetchFile error:', error);
            return { success: false, error: error.message };
        }
    }
};

module.exports = s3Service;
