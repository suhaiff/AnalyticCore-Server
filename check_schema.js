const mysql = require('mysql2');

const db = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: 'root',
  database: 'insightai'
});

db.connect((err) => {
  if (err) {
    console.error('Connection failed:', err.message);
    process.exit(1);
  }
  
  db.query('DESCRIBE dashboards', (err, results) => {
    if (err) {
      console.error('Error describing table:', err.message);
    } else {
      console.log('Dashboards Table Schema:', results);
    }
    db.end();
  });
});
