const XLSX = require('xlsx');
const https = require('https');
const fs = require('fs');

const fileUrl = 'https://docs.google.com/spreadsheets/d/1XUHj2c8vGZoGGI4GyfVR2Qk758FwTLWQK354YqIXrSE/export?format=xlsx';
const dest = 'features.xlsx';

const downloadFile = (url, dest) => {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    https.get(url, (response) => {
      response.pipe(file);
      file.on('finish', () => {
        file.close(resolve);
      });
    }).on('error', (err) => {
      fs.unlink(dest, () => reject(err));
    });
  });
};

const run = async () => {
  try {
    await downloadFile(fileUrl, dest);
    const workbook = XLSX.readFile(dest);
    console.log("Sheets:", workbook.SheetNames);
    
    for (const sheetName of workbook.SheetNames) {
      console.log(`\n--- Sheet: ${sheetName} ---`);
      const sheet = workbook.Sheets[sheetName];
      const data = XLSX.utils.sheet_to_json(sheet, { header: 1 });
      console.log(data.slice(0, 50));
    }
  } catch (e) {
    console.error(e);
  }
};

run();
