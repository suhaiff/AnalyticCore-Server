import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
dotenv.config();

const supabase = createClient(process.env.SUPABASE_URL, process.env.SUPABASE_KEY);

async function inspectFolders() {
    console.log('--- Inspecting Workspace Folders ---');
    const { data: folders, error: fError } = await supabase.from('workspace_folders').select('*');
    if (fError) {
        console.error('Folders Error:', fError);
        return;
    }
    
    folders.forEach(f => {
        console.log(`Folder: ${f.name} (ID: ${f.id})`);
        console.log(`  Owner ID: ${f.owner_id} (Type: ${typeof f.owner_id})`);
    });
    
    console.log('\n--- Inspecting Users ---');
    const { data: users, error: uError } = await supabase.from('users').select('id, name, email');
    if (uError) {
        console.error('Users Error:', uError);
        return;
    }
    users.forEach(u => {
        console.log(`User: ${u.name} (ID: ${u.id}) - ${u.email}`);
    });
}

inspectFolders();
