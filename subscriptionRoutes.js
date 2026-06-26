const express = require('express');
const router = express.Router();
const { supabase } = require('./supabaseService');

// ==========================================
// User Subscription APIs
// ==========================================

// Get all standalone features for the master library
router.get('/features', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('features')
      .select('*')
      .eq('is_active', true)
      .order('display_name', { ascending: true });

    if (error) throw error;
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch features', details: error.message });
  }
});

// Get available active plans
router.get('/plans', async (req, res) => {
  try {
    const { data, error } = await supabase
      .from('plans')
      .select('*')
      .eq('is_active', true)
      .order('monthly_price', { ascending: false });

    if (error) throw error;
    res.json(data);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch plans', details: error.message });
  }
});

// Get available active modules and their features
router.get('/modules', async (req, res) => {
  try {
    const { data: modules, error: modError } = await supabase
      .from('modules')
      .select(`
        id, name, description, icon, monthly_price, yearly_price, display_order,
        module_features(
          features (id, permission_key, display_name, description, monthly_price, yearly_price, is_active)
        )
      `)
      .eq('is_active', true)
      .order('display_order', { ascending: true });

    if (modError) throw modError;
    
    // Flatten the many-to-many relationship so it remains backward compatible with frontend
    const formattedModules = modules.map(mod => ({
      ...mod,
      features: (mod.module_features || [])
        .map(mf => mf.features)
        .filter(f => f && f.is_active)
    }));
    
    res.json(formattedModules);
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch modules', details: error.message });
  }
});

// Get subscription for organization
router.get('/:organization_id', async (req, res) => {
  const { organization_id } = req.params;
  try {
    const { data: subscription, error: subError } = await supabase
      .from('subscriptions')
      .select(`
        id, status, billing_cycle, current_period_end, cancel_at_period_end,
        plans (id, name, type),
        purchased_modules (module_id)
      `)
      .eq('organization_id', organization_id)
      .single();

    if (subError && subError.code !== 'PGRST116') {
      throw subError;
    }

    if (!subscription) {
      return res.json({ subscription: null });
    }

    // Format purchased modules as array of IDs
    const modules = subscription.purchased_modules.map(pm => pm.module_id);
    
    res.json({
      ...subscription,
      purchased_modules: modules
    });
  } catch (error) {
    res.status(500).json({ error: 'Failed to fetch subscription', details: error.message });
  }
});

// Create or update subscription
router.post('/', async (req, res) => {
  const { organization_id, plan_id, billing_cycle, module_ids } = req.body;
  
  if (!organization_id || !plan_id) {
    return res.status(400).json({ error: 'organization_id and plan_id are required' });
  }

  try {
    // 1. Check if org already has a subscription
    const { data: existingSub } = await supabase
      .from('subscriptions')
      .select('id')
      .eq('organization_id', organization_id)
      .single();

    let subscriptionId;

    if (existingSub) {
      // Update existing subscription
      const { data, error } = await supabase
        .from('subscriptions')
        .update({
          plan_id,
          billing_cycle,
          status: 'ACTIVE',
          updated_at: new Date()
        })
        .eq('id', existingSub.id)
        .select()
        .single();
      
      if (error) throw error;
      subscriptionId = data.id;
      
      // Delete existing purchased modules
      await supabase
        .from('purchased_modules')
        .delete()
        .eq('subscription_id', subscriptionId);
        
    } else {
      // Create new subscription
      const { data, error } = await supabase
        .from('subscriptions')
        .insert({
          organization_id,
          plan_id,
          billing_cycle,
          status: 'ACTIVE',
          current_period_start: new Date(),
          current_period_end: new Date(new Date().setMonth(new Date().getMonth() + (billing_cycle === 'YEARLY' ? 12 : 1)))
        })
        .select()
        .single();
        
      if (error) throw error;
      subscriptionId = data.id;
    }

    // Insert new purchased modules if Custom Plan
    if (module_ids && module_ids.length > 0) {
      const pmData = module_ids.map(id => ({
        subscription_id: subscriptionId,
        module_id: id
      }));
      
      const { error: pmError } = await supabase
        .from('purchased_modules')
        .insert(pmData);
        
      if (pmError) throw pmError;
    }

    res.json({ success: true, message: 'Subscription successfully updated', subscription_id: subscriptionId });
  } catch (error) {
    res.status(500).json({ error: 'Failed to process subscription', details: error.message });
  }
});

module.exports = router;
