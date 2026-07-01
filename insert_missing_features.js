require('dotenv').config();
const { supabase } = require('./supabaseService');
const fs = require('fs');

async function run() {
  const csv = fs.readFileSync('/home/Suhaif/Downloads/insightai_clone/Analytic-Core/features.csv', 'utf-8');
  const lines = csv.split('\n').filter(l => l.trim() !== '');
  
  // Skip header
  const records = lines.slice(1);
  
  let count = 0;
  for (const record of records) {
    const parts = record.split(',');
    if (parts.length < 3) continue;
    
    let module = parts[0].trim();
    let name = parts[1].trim();
    let description = parts.slice(2, parts.length - 1).join(',').replace(/^"|"$/g, '').trim();
    
    if (!name || name === 'Feature') continue;
    
    const permission_key = (module.toLowerCase().replace(/[^a-z0-9]+/g, '_') + '_' + name.toLowerCase().replace(/[^a-z0-9]+/g, '_')).substring(0, 50).replace(/_+$/, '');
    
    const { data: existing } = await supabase.from('features').select('id').eq('permission_key', permission_key).single();
    
    if (!existing) {
      const { error } = await supabase.from('features').insert({
        permission_key,
        display_name: `${module}: ${name}`,
        description: description,
        monthly_price: 0,
        yearly_price: 0
      });
      
      if (error) {
        console.error(`Failed to insert ${permission_key}:`, error.message);
      } else {
        console.log(`Inserted: ${permission_key}`);
        count++;
      }
    } else {
      console.log(`Skipped (already exists): ${permission_key}`);
    }
  }
  
  console.log(`\nFinished inserting ${count} new features.`);
}

run();
