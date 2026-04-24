require('dotenv').config();
const brevoService = require('./brevoService');

async function test() {
  console.log("Testing email with sender:", process.env.BREVO_SENDER_EMAIL);
  const success = await brevoService.sendTemporaryPasswordEmail('harishkadiravan.vtab@gmail.com', 'Harish', 'TestPass123');
  console.log("Success:", success);
}
test();
