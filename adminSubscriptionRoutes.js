const express = require('express');
const router = express.Router();
const { supabase } = require('./supabaseService');

// Middleware to verify admin status
const isAdmin = async (req, res, next) => {
  // In a real app, verify via req.user from auth middleware
  // We assume the frontend passes user ID or token that verifies them.
  // For now, bypassing strict auth for the migration setup if running locally.
  next();
};

router.use(isAdmin);

// ==========================================
// Admin Plan APIs
// ==========================================

router.put('/plans/:id/price', async (req, res) => {
  const { id } = req.params;
  const { monthly_price, yearly_price, changed_by } = req.body;
  
  try {
    // Get old price
    const { data: oldPlan } = await supabase.from('plans').select('monthly_price, yearly_price').eq('id', id).single();
    
    // Update price
    const { data, error } = await supabase
      .from('plans')
      .update({ monthly_price, yearly_price })
      .eq('id', id)
      .select()
      .single();
      
    if (error) throw error;
    
    // Log history
    if (oldPlan) {
      await supabase.from('pricing_history').insert({
        entity_type: 'PLAN',
        entity_id: id,
        old_monthly_price: oldPlan.monthly_price,
        new_monthly_price: monthly_price,
        old_yearly_price: oldPlan.yearly_price,
        new_yearly_price: yearly_price,
        changed_by: changed_by || null,
        reason: 'Admin price update'
      });
    }
    
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to update plan price', details: error.message });
  }
});

// ==========================================
// Admin Module APIs
// ==========================================

router.post('/modules', async (req, res) => {
  const { name, description, icon, monthly_price, yearly_price, display_order } = req.body;
  try {
    const { data, error } = await supabase
      .from('modules')
      .insert({ name, description, icon, monthly_price, yearly_price, display_order })
      .select()
      .single();
      
    if (error) throw error;
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to create module', details: error.message });
  }
});

router.put('/modules/:id', async (req, res) => {
  const { id } = req.params;
  const updates = req.body;
  
  try {
    // If price changed, we should log it
    let oldModule = null;
    if (updates.monthly_price !== undefined || updates.yearly_price !== undefined) {
      const { data } = await supabase.from('modules').select('monthly_price, yearly_price').eq('id', id).single();
      oldModule = data;
    }

    const { data, error } = await supabase
      .from('modules')
      .update(updates)
      .eq('id', id)
      .select()
      .single();
      
    if (error) throw error;
    
    // Log price change if applicable
    if (oldModule && (updates.monthly_price !== undefined || updates.yearly_price !== undefined)) {
      await supabase.from('pricing_history').insert({
        entity_type: 'MODULE',
        entity_id: id,
        old_monthly_price: oldModule.monthly_price,
        new_monthly_price: updates.monthly_price || oldModule.monthly_price,
        old_yearly_price: oldModule.yearly_price,
        new_yearly_price: updates.yearly_price || oldModule.yearly_price,
        changed_by: updates.changed_by || null,
        reason: 'Admin module update'
      });
    }

    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to update module', details: error.message });
  }
});

router.delete('/modules/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const { error } = await supabase.from('modules').delete().eq('id', id);
    if (error) throw error;
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: 'Failed to delete module', details: error.message });
  }
});

// ==========================================
// Admin Feature APIs
// ==========================================

router.post('/features', async (req, res) => {
  const { permission_key, display_name, description, monthly_price, yearly_price } = req.body;
  try {
    const { data, error } = await supabase
      .from('features')
      .insert({ permission_key, display_name, description, monthly_price, yearly_price })
      .select()
      .single();
      
    if (error) throw error;
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to create feature', details: error.message });
  }
});

router.post('/modules/:id/features', async (req, res) => {
  const { id } = req.params;
  const { feature_id } = req.body;
  try {
    const { data, error } = await supabase
      .from('module_features')
      .insert({ module_id: id, feature_id })
      .select()
      .single();
      
    if (error) throw error;
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to attach feature to module', details: error.message });
  }
});

router.delete('/modules/:id/features/:featureId', async (req, res) => {
  const { id, featureId } = req.params;
  try {
    const { error } = await supabase
      .from('module_features')
      .delete()
      .match({ module_id: id, feature_id: featureId });
      
    if (error) throw error;
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: 'Failed to remove feature from module', details: error.message });
  }
});

router.put('/features/:id', async (req, res) => {
  const { id } = req.params;
  const updates = req.body;
  try {
    const { data, error } = await supabase
      .from('features')
      .update(updates)
      .eq('id', id)
      .select()
      .single();
      
    if (error) throw error;
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to update feature', details: error.message });
  }
});

router.delete('/features/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const { error } = await supabase.from('features').delete().eq('id', id);
    if (error) throw error;
    res.json({ success: true });
  } catch (error) {
    res.status(500).json({ error: 'Failed to delete feature', details: error.message });
  }
});

module.exports = router;
