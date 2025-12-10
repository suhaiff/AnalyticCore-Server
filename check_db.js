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
  console.log('Connected to DB');
  
  db.query('SHOW TABLES', (err, results) => {
    if (err) {
      console.error('Error showing tables:', err.message);
    } else {
      console.log('Tables:', results.map(r => Object.values(r)[0]));
    }
    db.end();
  });
});
