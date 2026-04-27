const axios = require('axios');

const ML_SERVICE_URL = process.env.ML_SERVICE_URL || 'http://localhost:8001';

// Helper function to handle ML service errors
const handleMLError = (error, res) => {
    if (error.code === 'ECONNREFUSED') {
        return res.status(503).json({
            ok: false,
            error: 'ML service is not running',
            url: ML_SERVICE_URL,
            service: null
        });
    }
    if (error.response) {
        return res.status(error.response.status || 500).json({
            ok: false,
            error: error.response.data?.error || error.message
        });
    }
    return res.status(500).json({
        ok: false,
        error: error.message || 'Failed to connect to ML service'
    });
};

module.exports = (app) => {
    // ML Health Check
    app.get('/api/ml/health', async (req, res) => {
        try {
            const response = await axios.get(`${ML_SERVICE_URL}/health`);
            res.json({
                ok: true,
                service: response.data,
                url: ML_SERVICE_URL
            });
        } catch (error) {
            handleMLError(error, res);
        }
    });

    // List available algorithms
    app.get('/api/ml/algorithms', async (req, res) => {
        try {
            const response = await axios.get(`${ML_SERVICE_URL}/algorithms`);
            res.json(response.data);
        } catch (error) {
            handleMLError(error, res);
        }
    });

    // List user's models
    app.get('/api/ml/models', async (req, res) => {
        try {
            const { userId } = req.query;
            if (!userId) {
                return res.status(400).json({ error: 'userId is required' });
            }
            const response = await axios.get(`${ML_SERVICE_URL}/models`, {
                params: { userId }
            });
            res.json(response.data);
        } catch (error) {
            handleMLError(error, res);
        }
    });

    // Get model details
    app.get('/api/ml/models/:modelId', async (req, res) => {
        try {
            const { modelId } = req.params;
            const { userId } = req.query;
            if (!userId) {
                return res.status(400).json({ error: 'userId is required' });
            }
            const response = await axios.get(`${ML_SERVICE_URL}/models/${modelId}`, {
                params: { userId }
            });
            res.json(response.data);
        } catch (error) {
            handleMLError(error, res);
        }
    });

    // Train a new model
    app.post('/api/ml/train', async (req, res) => {
        try {
            // Forward the request with file to ML service
            const form = new FormData();
            
            // Copy all fields from request
            for (const key in req.body) {
                form.append(key, req.body[key]);
            }

            // If there's a file, add it
            if (req.file) {
                form.append('file', req.file.buffer, req.file.originalname);
            }

            const response = await axios.post(`${ML_SERVICE_URL}/train`, form, {
                headers: form.getHeaders?.() || { 'Content-Type': 'multipart/form-data' },
                timeout: 10 * 60 * 1000 // 10 minutes
            });
            res.json(response.data);
        } catch (error) {
            handleMLError(error, res);
        }
    });

    // Run predictions
    app.post('/api/ml/predict', async (req, res) => {
        try {
            const form = new FormData();
            
            // Copy all fields
            for (const key in req.body) {
                form.append(key, req.body[key]);
            }

            // If there's a file, add it
            if (req.file) {
                form.append('file', req.file.buffer, req.file.originalname);
            }

            const response = await axios.post(`${ML_SERVICE_URL}/predict`, form, {
                headers: form.getHeaders?.() || { 'Content-Type': 'multipart/form-data' },
                timeout: 10 * 60 * 1000
            });
            res.json(response.data);
        } catch (error) {
            handleMLError(error, res);
        }
    });

    // Delete model
    app.delete('/api/ml/models/:modelId', async (req, res) => {
        try {
            const { modelId } = req.params;
            const { userId } = req.query;
            if (!userId) {
                return res.status(400).json({ error: 'userId is required' });
            }
            const response = await axios.delete(`${ML_SERVICE_URL}/models/${modelId}`, {
                params: { userId }
            });
            res.json(response.data);
        } catch (error) {
            handleMLError(error, res);
        }
    });

    // Retrain model
    app.post('/api/ml/retrain/:modelId', async (req, res) => {
        try {
            const { modelId } = req.params;
            const form = new FormData();
            
            // Copy all fields
            for (const key in req.body) {
                form.append(key, req.body[key]);
            }

            // If there's a file, add it
            if (req.file) {
                form.append('file', req.file.buffer, req.file.originalname);
            }

            const response = await axios.post(`${ML_SERVICE_URL}/models/${modelId}/retrain`, form, {
                headers: form.getHeaders?.() || { 'Content-Type': 'multipart/form-data' },
                timeout: 10 * 60 * 1000
            });
            res.json(response.data);
        } catch (error) {
            handleMLError(error, res);
        }
    });
};
