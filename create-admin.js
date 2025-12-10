const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const dataverseService = require('./dataverseService');

async function createAdminUser() {
    console.log('Creating admin user in Dataverse...\n');

    const adminData = {
        name: 'Suhaif',
        email: 'sohib.vtab@gmail.com',
        password: 'Admin123',
        role: 'ADMIN'
    };

    try {
        // Check if user already exists
        console.log(`Checking if user ${adminData.email} already exists...`);
        const existingUser = await dataverseService.getUserByEmail(adminData.email);

        if (existingUser) {
            console.log('\n❌ User already exists with the following details:');
            console.log(`   ID: ${existingUser.id}`);
            console.log(`   Name: ${existingUser.name}`);
            console.log(`   Email: ${existingUser.email}`);
            console.log(`   Role: ${existingUser.role}`);
            console.log('\nℹ️  If you need to update the user, please delete them from Dataverse first.');
            return;
        }

        // Create the admin user
        console.log('User does not exist. Creating new admin user...\n');
        const newUser = await dataverseService.createUser(
            adminData.name,
            adminData.email,
            adminData.password,
            adminData.role
        );

        console.log('✅ Admin user created successfully!\n');
        console.log('User Details:');
        console.log('─────────────────────────────');
        console.log(`Name:     ${newUser.name}`);
        console.log(`Email:    ${newUser.email}`);
        console.log(`Role:     ${newUser.role}`);
        console.log(`ID:       ${newUser.id}`);
        console.log('─────────────────────────────\n');

        console.log('You can now login with these credentials:');
        console.log(`  Email:    ${adminData.email}`);
        console.log(`  Password: ${adminData.password}`);
        console.log('\n🎉 Admin account is ready to use!');

    } catch (error) {
        console.error('\n❌ Error creating admin user:');
        console.error(error.message);

        if (error.response) {
            console.error('\nAPI Response Error:');
            console.error('Status:', error.response.status);
            console.error('Data:', JSON.stringify(error.response.data, null, 2));
        }

        console.log('\nPossible issues:');
        console.log('  1. Check that your Dataverse credentials in .env are correct');
        console.log('  2. Verify that the entity "crc6f_hr_userses" exists in Dataverse');
        console.log('  3. Ensure the Azure AD app has proper permissions');
        console.log('  4. Check that all required fields exist in the entity');

        process.exit(1);
    }
}

// Run the function
console.log('═══════════════════════════════════════════════════');
console.log('   Dataverse Admin User Creation Script');
console.log('═══════════════════════════════════════════════════\n');

createAdminUser()
    .then(() => {
        console.log('\n✓ Script completed successfully');
        process.exit(0);
    })
    .catch((error) => {
        console.error('\n✗ Script failed:', error.message);
        process.exit(1);
    });
