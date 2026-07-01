require('dotenv').config();
const { supabase } = require('./supabaseService');
async function run() {
  const { data, error } = await supabase.from('features').select('permission_key');
  console.log(data);
}
run();
