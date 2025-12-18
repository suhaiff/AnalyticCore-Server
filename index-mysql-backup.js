const express = require('express');
const mysql = require('mysql2');
const cors = require('cors');
const bodyParser = require('body-parser');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
const XLSX = require('xlsx');

const app = express();
const port = 3001;

app.use(cors());
app.use(bodyParser.json());

// Ensure uploads directory exists
const uploadDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadDir)) {
  fs.mkdirSync(uploadDir);
}

// Configure Multer
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, uploadDir)
  },
  filename: function (req, file, cb) {
    const uniqueSuffix = Date.now() + '-' + Math.round(Math.random() * 1E9)
    cb(null, uniqueSuffix + '-' + file.originalname)
  }
})

const upload = multer({ storage: storage });

const db = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'root', // Replace with your MySQL password
  database: 'insightai'
});

db.connect((err) => {
  if (err) {
    console.error('Error connecting to MySQL:', err);
    return;
  }
  console.log('Connected to MySQL database');
});

// Auth Endpoints
app.post('/api/login', (req, res) => {
  const { email, password } = req.body;
  const query = 'SELECT * FROM users WHERE email = ? AND password = ?';
  db.query(query, [email, password], (err, results) => {
    if (err) return res.status(500).json({ error: err.message });
    if (results.length > 0) {
      const user = results[0];
      // Don't send password back
      const { password, ...userWithoutPassword } = user;
      res.json(userWithoutPassword);
    } else {
      res.status(401).json({ error: 'Invalid credentials' });
    }
  });
});

app.post('/api/signup', (req, res) => {
  const { name, email, password } = req.body;
  const query = 'INSERT INTO users (name, email, password, role) VALUES (?, ?, ?, ?)';
  // Default role is USER. First user created via SQL script is ADMIN.
  db.query(query, [name, email, password, 'USER'], (err, result) => {
    if (err) {
      if (err.code === 'ER_DUP_ENTRY') {
        return res.status(400).json({ error: 'Email already exists' });
      }
      return res.status(500).json({ error: err.message });
    }
    const newUser = { id: result.insertId, name, email, role: 'USER' };
    res.json(newUser);
  });
});

// User Management (Admin)
app.get('/api/users', (req, res) => {
  const query = 'SELECT id, name, email, role, created_at FROM users';
  db.query(query, (err, results) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json(results);
  });
});

app.delete('/api/users/:id', (req, res) => {
  const userId = req.params.id;
  const query = 'DELETE FROM users WHERE id = ?';
  db.query(query, [userId], (err, result) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json({ message: 'User deleted successfully' });
  });
});

// Dashboard Endpoints
app.post('/api/dashboards', (req, res) => {
  const { userId, dashboard } = req.body;

  if (!dashboard) {
    return res.status(400).json({ error: 'Missing dashboard data' });
  }

  const { name, dataModel, chartConfigs } = dashboard;

  const query = 'INSERT INTO dashboards (user_id, name, data_model, chart_configs) VALUES (?, ?, ?, ?)';
  db.query(query, [userId, name, JSON.stringify(dataModel), JSON.stringify(chartConfigs)], (err, result) => {
    if (err) {
      console.error("Save Dashboard DB Error:", err);
      return res.status(500).json({ error: err.message });
    }
    res.json({ id: result.insertId, message: 'Dashboard saved' });
  });
});

app.get('/api/dashboards', (req, res) => {
  const userId = req.query.userId;
  const query = 'SELECT * FROM dashboards WHERE user_id = ? ORDER BY created_at DESC';
  db.query(query, [userId], (err, results) => {
    if (err) return res.status(500).json({ error: err.message });

    const dashboards = results.map(row => ({
      id: row.id.toString(),
      name: row.name,
      date: new Date(row.created_at).toLocaleDateString(),
      dataModel: typeof row.data_model === 'string' ? JSON.parse(row.data_model) : row.data_model,
      chartConfigs: typeof row.chart_configs === 'string' ? JSON.parse(row.chart_configs) : row.chart_configs
    }));
    res.json(dashboards);
  });
});

app.delete('/api/dashboards/:id', (req, res) => {
  const id = req.params.id;
  const query = 'DELETE FROM dashboards WHERE id = ?';
  db.query(query, [id], (err, result) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json({ message: 'Dashboard deleted' });
  });
});

// Admin: Get All Dashboards
app.get('/api/admin/dashboards', (req, res) => {
  const query = `
    SELECT d.*, u.name as user_name 
    FROM dashboards d 
    JOIN users u ON d.user_id = u.id 
    ORDER BY d.created_at DESC
  `;
  db.query(query, (err, results) => {
    if (err) return res.status(500).json({ error: err.message });

    const dashboards = results.map(row => ({
      id: row.id.toString(),
      name: row.name,
      userName: row.user_name,
      date: new Date(row.created_at).toLocaleDateString(),
      dataModel: typeof row.data_model === 'string' ? JSON.parse(row.data_model) : row.data_model,
      chartConfigs: typeof row.chart_configs === 'string' ? JSON.parse(row.chart_configs) : row.chart_configs
    }));
    res.json(dashboards);
  });
});

// File Upload Endpoints
app.post('/api/upload', upload.single('file'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No file uploaded' });
  }

  const { userId } = req.body;
  const { filename, originalname, path: filePath, mimetype, size } = req.file;

  try {
    // Check if it's an Excel file
    const isExcelFile = mimetype.includes('spreadsheet') || mimetype.includes('excel') ||
      filename.endsWith('.xlsx') || filename.endsWith('.xls');

    if (!isExcelFile) {
      return res.status(400).json({ error: 'Only Excel files are supported' });
    }

    // Read the Excel file
    const uploadedWorkbook = XLSX.readFile(filePath);
    const sheetNames = uploadedWorkbook.SheetNames;
    const sheetCount = sheetNames.length;

    // Start a transaction
    db.beginTransaction(async (transErr) => {
      if (transErr) {
        console.error("Transaction Error:", transErr);
        return res.status(500).json({ error: 'Failed to start transaction' });
      }

      try {
        // 1. Insert file metadata
        const fileQuery = 'INSERT INTO uploaded_files (user_id, original_name, mime_type, file_size, sheet_count) VALUES (?, ?, ?, ?, ?)';

        db.query(fileQuery, [userId, originalname, mimetype, size, sheetCount], async (fileErr, fileResult) => {
          if (fileErr) {
            return db.rollback(() => {
              console.error("File Insert Error:", fileErr);
              res.status(500).json({ error: fileErr.message });
            });
          }

          const fileId = fileResult.insertId;

          try {
            // 2. Process each sheet
            for (let i = 0; i < sheetNames.length; i++) {
              const sheetName = sheetNames[i];
              const worksheet = uploadedWorkbook.Sheets[sheetName];

              // Convert sheet to array of arrays
              const sheetData = XLSX.utils.sheet_to_json(worksheet, { header: 1, defval: null });
              const rowCount = sheetData.length;
              const columnCount = rowCount > 0 ? Math.max(...sheetData.map(row => row.length)) : 0;

              // Insert sheet metadata
              const sheetQuery = 'INSERT INTO excel_sheets (file_id, sheet_name, sheet_index, row_count, column_count) VALUES (?, ?, ?, ?, ?)';

              await new Promise((resolve, reject) => {
                db.query(sheetQuery, [fileId, sheetName, i, rowCount, columnCount], (sheetErr, sheetResult) => {
                  if (sheetErr) {
                    reject(sheetErr);
                    return;
                  }

                  const sheetId = sheetResult.insertId;

                  // Insert row data in batches
                  if (rowCount > 0) {
                    const dataValues = [];
                    for (let rowIndex = 0; rowIndex < sheetData.length; rowIndex++) {
                      const rowData = sheetData[rowIndex];
                      dataValues.push([sheetId, rowIndex, JSON.stringify(rowData)]);
                    }

                    // Batch insert rows
                    const dataQuery = 'INSERT INTO excel_data (sheet_id, row_index, row_data) VALUES ?';
                    db.query(dataQuery, [dataValues], (dataErr) => {
                      if (dataErr) {
                        reject(dataErr);
                        return;
                      }
                      resolve();
                    });
                  } else {
                    resolve();
                  }
                });
              });
            }

            // 3. Log to file_upload_log
            const now = new Date();
            const logQuery = 'INSERT INTO file_upload_log (file_id, upload_date, upload_time, file_path, status) VALUES (?, ?, ?, ?, ?)';
            await new Promise((resolve, reject) => {
              db.query(logQuery, [fileId, now.toISOString().split('T')[0], now.toTimeString().split(' ')[0], filePath, 'SUCCESS'], (logErr) => {
                if (logErr) reject(logErr);
                else resolve();
              });
            });

            // 4. Legacy Excel logging (optional - keep for backward compatibility)
            try {
              const logFilePath = path.join(__dirname, '..', 'user file log.xlsx');
              let workbook;

              if (fs.existsSync(logFilePath)) {
                workbook = XLSX.readFile(logFilePath);
              } else {
                workbook = XLSX.utils.book_new();
              }

              let uploadsData = [];
              if (workbook.SheetNames.includes('Uploads')) {
                uploadsData = XLSX.utils.sheet_to_json(workbook.Sheets['Uploads']);
              }

              const newUploadRow = {
                'S.No': uploadsData.length + 1,
                'Path': filePath,
                'File Type': mimetype,
                'File name': originalname,
                'Uploaded date': now.toLocaleDateString(),
                'Uploaded time': now.toLocaleTimeString()
              };
              uploadsData.push(newUploadRow);

              const uploadsSheet = XLSX.utils.json_to_sheet(uploadsData);
              if (workbook.SheetNames.includes('Uploads')) {
                workbook.Sheets['Uploads'] = uploadsSheet;
              } else {
                XLSX.utils.book_append_sheet(workbook, uploadsSheet, 'Uploads');
              }

              let fileDetailsData = [];
              if (workbook.SheetNames.includes('File Details')) {
                fileDetailsData = XLSX.utils.sheet_to_json(workbook.Sheets['File Details']);
              }

              sheetNames.forEach(sheetName => {
                const newDetailRow = {
                  'S.No': fileDetailsData.length + 1,
                  'Path': filePath,
                  'File name': originalname,
                  'Sheet count': sheetCount,
                  'Sheet Name': sheetName
                };
                fileDetailsData.push(newDetailRow);
              });

              const fileDetailsSheet = XLSX.utils.json_to_sheet(fileDetailsData);
              if (workbook.SheetNames.includes('File Details')) {
                workbook.Sheets['File Details'] = fileDetailsSheet;
              } else {
                XLSX.utils.book_append_sheet(workbook, fileDetailsSheet, 'File Details');
              }

              XLSX.writeFile(workbook, logFilePath);
              console.log('Logged upload to Excel:', logFilePath);
            } catch (logErr) {
              console.error("Error logging to Excel:", logErr);
            }

            // Commit transaction
            db.commit((commitErr) => {
              if (commitErr) {
                return db.rollback(() => {
                  console.error("Commit Error:", commitErr);
                  res.status(500).json({ error: 'Failed to commit transaction' });
                });
              }

              // Delete the physical file after successful database storage
              try {
                fs.unlinkSync(filePath);
                console.log('Deleted physical file:', filePath);
              } catch (deleteErr) {
                console.error('Error deleting file:', deleteErr);
                // Don't fail the request if file deletion fails
              }

              res.json({
                message: 'File uploaded and data stored successfully',
                file: {
                  id: fileId,
                  originalName: originalname,
                  sheetCount: sheetCount,
                  sheets: sheetNames
                }
              });
            });

          } catch (processErr) {
            db.rollback(() => {
              console.error("Processing Error:", processErr);
              res.status(500).json({ error: 'Failed to process file data: ' + processErr.message });
            });
          }
        });

      } catch (err) {
        db.rollback(() => {
          console.error("Transaction Error:", err);
          res.status(500).json({ error: err.message });
        });
      }
    });

  } catch (err) {
    console.error("Upload Error:", err);
    res.status(500).json({ error: 'Failed to upload file: ' + err.message });
  }
});

// Log Configuration Endpoint
app.post('/api/log-config', (req, res) => {
  const { fileName, columns, joinConfigs } = req.body;

  try {
    const logFilePath = path.join(__dirname, '..', 'user file log.xlsx');
    let workbook;

    if (fs.existsSync(logFilePath)) {
      workbook = XLSX.readFile(logFilePath);
    } else {
      workbook = XLSX.utils.book_new();
    }

    const now = new Date();
    let configData = [];

    if (workbook.SheetNames.includes('Configuration Logs')) {
      configData = XLSX.utils.sheet_to_json(workbook.Sheets['Configuration Logs']);
    }

    const joinConfigString = (joinConfigs && joinConfigs.length > 0)
      ? joinConfigs.map(j => `${j.leftTableId}.${j.leftKey} ${j.type} JOIN ${j.rightTableId}.${j.rightKey}`).join('; ')
      : "no join configs";

    const newRow = {
      'S.No': configData.length + 1,
      'File Name': fileName,
      'Date': now.toLocaleDateString(),
      'Time': now.toLocaleTimeString(),
      'Columns': Array.isArray(columns) ? columns.join(', ') : columns,
      'Join Configs': joinConfigString
    };

    configData.push(newRow);
    const newSheet = XLSX.utils.json_to_sheet(configData);

    if (workbook.SheetNames.includes('Configuration Logs')) {
      workbook.Sheets['Configuration Logs'] = newSheet;
    } else {
      XLSX.utils.book_append_sheet(workbook, newSheet, 'Configuration Logs');
    }

    XLSX.writeFile(workbook, logFilePath);
    console.log('Logged configuration to Excel');
    res.json({ message: 'Configuration logged successfully' });

  } catch (err) {
    console.error("Error logging configuration:", err);
    res.status(500).json({ error: 'Failed to log configuration' });
  }
});

// Admin: Get All Uploads
app.get('/api/admin/uploads', (req, res) => {
  const query = `
    SELECT uf.*, u.name as user_name, u.email as user_email,
           (SELECT COUNT(*) FROM excel_sheets WHERE file_id = uf.id) as actual_sheet_count,
           (SELECT SUM(row_count) FROM excel_sheets WHERE file_id = uf.id) as total_row_count
    FROM uploaded_files uf
    JOIN users u ON uf.user_id = u.id
    ORDER BY uf.created_at DESC
  `;
  db.query(query, (err, results) => {
    if (err) return res.status(500).json({ error: err.message });

    // Map results to match expected format
    const mappedResults = results.map(row => ({
      id: row.id,
      user_id: row.user_id,
      filename: row.original_name, // for backward compatibility
      original_name: row.original_name,
      file_path: 'database', // indicate it's stored in database
      mime_type: row.mime_type,
      size: row.file_size,
      created_at: row.created_at,
      user_name: row.user_name,
      user_email: row.user_email,
      sheet_count: row.sheet_count,
      total_rows: row.total_row_count || 0
    }));

    res.json(mappedResults);
  });
});

// Get File Content (for Preview) - Now reads from database
app.get('/api/uploads/:id/content', (req, res) => {
  const id = req.params.id;

  // Get file metadata
  const fileQuery = 'SELECT * FROM uploaded_files WHERE id = ?';

  db.query(fileQuery, [id], (fileErr, fileResults) => {
    if (fileErr) return res.status(500).json({ error: fileErr.message });
    if (fileResults.length === 0) return res.status(404).json({ error: 'File not found' });

    const fileRecord = fileResults[0];

    // Get all sheets for this file
    const sheetsQuery = 'SELECT * FROM excel_sheets WHERE file_id = ? ORDER BY sheet_index';

    db.query(sheetsQuery, [id], (sheetsErr, sheetsResults) => {
      if (sheetsErr) return res.status(500).json({ error: sheetsErr.message });

      console.log('Found', sheetsResults.length, 'sheets for file', fileRecord.original_name);

      // For each sheet, get its data
      const sheetPromises = sheetsResults.map(sheet => {
        return new Promise((resolve, reject) => {
          const dataQuery = 'SELECT row_index, row_data FROM excel_data WHERE sheet_id = ? ORDER BY row_index';

          db.query(dataQuery, [sheet.id], (dataErr, dataResults) => {
            if (dataErr) {
              reject(dataErr);
              return;
            }

            // Parse row data from JSON
            const sheetData = dataResults.map(row => {
              try {
                return JSON.parse(row.row_data);
              } catch (parseErr) {
                console.error('Error parsing row data:', parseErr);
                return [];
              }
            });

            console.log(`Sheet "${sheet.sheet_name}" has ${sheetData.length} rows`);

            resolve({
              name: sheet.sheet_name,
              data: sheetData
            });
          });
        });
      });

      Promise.all(sheetPromises)
        .then(sheets => {
          console.log('Sending response with', sheets.length, 'sheets');
          res.json({
            fileName: fileRecord.original_name,
            sheets: sheets
          });
        })
        .catch(err => {
          console.error("Error retrieving sheet data:", err);
          res.status(500).json({ error: 'Failed to retrieve file content' });
        });
    });
  });
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
