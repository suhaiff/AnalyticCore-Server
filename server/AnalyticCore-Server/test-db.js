const { createClient } = require('@supabase/supabase-js');
require('dotenv').config({path: '../.env'});

const supabaseUrl = process.env.VITE_SUPABASE_URL || 'https://hllcxyhlzibgffvylnyl.supabase.co';
const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY || process.env.VITE_SUPABASE_ANON_KEY;
const supabase = createClient(supabaseUrl, supabaseKey);

async function test() {
  const { data, error } = await supabase.from('organizations').select('*');
  console.log("Error:", error);
  console.log("Data:", data);
}
test();
