const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });
const supabaseService = require('./supabaseService');

async function testConnection() {
    console.log('Testing Supabase connection...');
    console.log('URL:', process.env.SUPABASE_URL);

    try {
        const users = await supabaseService.getUsers();
        console.log('✓ Connection successful!');
        console.log(`✓ Retrieved ${users.length} users`);
        if (users.length > 0) {
            console.log('First user:', users[0].email);
        }
        process.exit(0);
    } catch (error) {
        console.error('✗ Connection failed:', error.message);
        if (error.code) console.error('Error code:', error.code);
        if (error.details) console.error('Error details:', error.details);
        process.exit(1);
    }
}

testConnection();
