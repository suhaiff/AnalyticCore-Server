require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseKey = process.env.SUPABASE_KEY;

if (!supabaseUrl || !supabaseKey) {
  console.error("Missing Supabase credentials in .env");
  process.exit(1);
}

const supabase = createClient(supabaseUrl, supabaseKey);

const newModules = [
  {
    name: "Data Integration Pro",
    description: "Connect to live databases, SharePoint, and extensive webhooks.",
    icon: "Database",
    monthly_price: 149.00,
    yearly_price: 1490.00,
    display_order: 4,
    features: [
      { display_name: "SharePoint Sync", permission_key: "integration_sharepoint" },
      { display_name: "Live Database Connections", permission_key: "integration_live_db" },
      { display_name: "Google Sheets Sync", permission_key: "integration_gsheets" }
    ]
  },
  {
    name: "Document AI",
    description: "Extract insights and query unstructured text (PDFs, Images) using advanced LLMs.",
    icon: "FileText",
    monthly_price: 199.00,
    yearly_price: 1990.00,
    display_order: 5,
    features: [
      { display_name: "PDF Analytics", permission_key: "docai_pdf" },
      { display_name: "Image Text Extraction (OCR)", permission_key: "docai_ocr" },
      { display_name: "Unstructured Data Querying", permission_key: "docai_query" }
    ]
  },
  {
    name: "Automation Engine",
    description: "Automate repetitive data tasks, email reporting, and trigger workflows.",
    icon: "Zap",
    monthly_price: 89.00,
    yearly_price: 890.00,
    display_order: 6,
    features: [
      { display_name: "Automated Email Reports", permission_key: "auto_reports" },
      { display_name: "Webhook Triggers", permission_key: "auto_webhooks" },
      { display_name: "Scheduled Data Refresh", permission_key: "auto_refresh" }
    ]
  }
];

const existingModulesFeatures = {
  "Dashboard Management": [
    { display_name: "Unlimited Dashboards", permission_key: "dash_unlimited" },
    { display_name: "Custom Themes", permission_key: "dash_themes" },
    { display_name: "Shareable Public Links", permission_key: "dash_public_link" }
  ],
  "AI Analytics": [
    { display_name: "Auto-ML Predictions", permission_key: "ai_predictions" },
    { display_name: "Natural Language Queries", permission_key: "ai_nlq" },
    { display_name: "Anomaly Detection", permission_key: "ai_anomaly" }
  ],
  "Report Builder": [
    { display_name: "Export to PDF/Excel", permission_key: "report_export" },
    { display_name: "White-label Reports", permission_key: "report_whitelabel" }
  ]
};

async function seed() {
  console.log("Seeding additional modules and features...");
  
  // 1. Insert new modules
  for (const mod of newModules) {
    const { features, ...modData } = mod;
    
    // Check if module exists
    const { data: existingMod } = await supabase.from('modules').select('id').eq('name', mod.name).single();
    let modId;
    
    if (!existingMod) {
      console.log(`Inserting module: ${mod.name}`);
      const { data, error } = await supabase.from('modules').insert([modData]).select().single();
      if (error) { console.error("Error inserting module:", error); continue; }
      modId = data.id;
    } else {
      modId = existingMod.id;
    }

    // Insert features for this module
    for (const feat of features) {
      await supabase.from('features').insert([{ module_id: modId, ...feat }]).select();
    }
  }

  // 2. Add features to existing modules
  const { data: allMods } = await supabase.from('modules').select('id, name');
  if (allMods) {
    for (const existingMod of allMods) {
      const featsToAdd = existingModulesFeatures[existingMod.name];
      if (featsToAdd) {
        for (const feat of featsToAdd) {
          // Check if feature exists
          const { data: existingFeat } = await supabase.from('features').select('id').eq('permission_key', feat.permission_key).single();
          if (!existingFeat) {
             console.log(`Adding feature ${feat.display_name} to ${existingMod.name}`);
             await supabase.from('features').insert([{ module_id: existingMod.id, ...feat }]);
          }
        }
      }
    }
  }
  
  console.log("Seeding complete!");
}

seed().catch(console.error);
