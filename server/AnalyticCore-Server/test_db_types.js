require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');
const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);
async function test() {
    console.log("Testing owner_id column type...");
    // Let's just insert a dummy failing row to see what column postgres complains about.
    const { data, error } = await supabase.from('workspace_folders').select('owner_id, id').limit(1);
    console.log("Select workspace_folders:", error);

    const { error: e2 } = await supabase.from('workspace_folder_access').select('user_id, folder_id').limit(1);
    console.log("Select workspace_folder_access:", e2);

    // Let's test the specific query to see which one fails
    const { error: e3 } = await supabase.from('workspace_folders').select('*').eq('owner_id', '2');
    console.log("Eq owner_id:", e3);

    const { error: e4 } = await supabase.from('workspace_folder_access').select('*').eq('user_id', '2');
    console.log("Eq user_id:", e4);
}
test();
