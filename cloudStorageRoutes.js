const express = require('express');
const router = express.Router();
const s3Service = require('./s3Service');
const azureBlobService = require('./azureBlobService');
const gcsService = require('./gcsService');

// ==========================================
// AWS S3 Endpoints
// ==========================================
router.post('/s3/test', async (req, res) => {
    const { credentials, bucket } = req.body;
    if (!credentials || !bucket) return res.status(400).json({ error: 'Missing credentials or bucket' });
    const result = await s3Service.testConnection(credentials, bucket);
    if (result.success) res.json({ success: true });
    else res.status(400).json({ error: result.error });
});

router.post('/s3/list-files', async (req, res) => {
    const { credentials, bucket } = req.body;
    const result = await s3Service.listFiles(credentials, bucket);
    if (result.success) res.json({ files: result.files });
    else res.status(400).json({ error: result.error });
});

router.post('/s3/fetch-file', async (req, res) => {
    const { credentials, bucket, fileKey } = req.body;
    const result = await s3Service.fetchFile(credentials, bucket, fileKey);
    if (result.success) {
        res.setHeader('Content-Type', result.mimeType || 'application/octet-stream');
        res.send(result.buffer);
    } else {
        res.status(400).json({ error: result.error });
    }
});

// ==========================================
// Azure Blob Storage Endpoints
// ==========================================
router.post('/azure/test', async (req, res) => {
    const { connectionString, containerName } = req.body;
    if (!connectionString || !containerName) return res.status(400).json({ error: 'Missing connection string or container' });
    const result = await azureBlobService.testConnection(connectionString, containerName);
    if (result.success) res.json({ success: true });
    else res.status(400).json({ error: result.error });
});

router.post('/azure/list-files', async (req, res) => {
    const { connectionString, containerName } = req.body;
    const result = await azureBlobService.listFiles(connectionString, containerName);
    if (result.success) res.json({ files: result.files });
    else res.status(400).json({ error: result.error });
});

router.post('/azure/fetch-file', async (req, res) => {
    const { connectionString, containerName, blobName } = req.body;
    const result = await azureBlobService.fetchFile(connectionString, containerName, blobName);
    if (result.success) {
        res.setHeader('Content-Type', result.mimeType || 'application/octet-stream');
        res.send(result.buffer);
    } else {
        res.status(400).json({ error: result.error });
    }
});

// ==========================================
// Google Cloud Storage Endpoints
// ==========================================
router.post('/gcs/test', async (req, res) => {
    const { credentialsJson, bucketName } = req.body;
    if (!credentialsJson || !bucketName) return res.status(400).json({ error: 'Missing credentials JSON or bucket name' });
    const result = await gcsService.testConnection(credentialsJson, bucketName);
    if (result.success) res.json({ success: true });
    else res.status(400).json({ error: result.error });
});

router.post('/gcs/list-files', async (req, res) => {
    const { credentialsJson, bucketName } = req.body;
    const result = await gcsService.listFiles(credentialsJson, bucketName);
    if (result.success) res.json({ files: result.files });
    else res.status(400).json({ error: result.error });
});

router.post('/gcs/fetch-file', async (req, res) => {
    const { credentialsJson, bucketName, fileName } = req.body;
    const result = await gcsService.fetchFile(credentialsJson, bucketName, fileName);
    if (result.success) {
        res.setHeader('Content-Type', result.mimeType || 'application/octet-stream');
        res.send(result.buffer);
    } else {
        res.status(400).json({ error: result.error });
    }
});

module.exports = router;
