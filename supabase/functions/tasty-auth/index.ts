import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: corsHeaders });

  try {
    const { username, password, mfa_code, user_id } = await req.json();
    if (!username || !password || !user_id) {
      return new Response(JSON.stringify({ error: 'Missing fields' }), {
        status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    const supabase = createClient(
      Deno.env.get('SUPABASE_URL')!,
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
    );

    // Build session payload
    const payload: Record<string, unknown> = { login: username, password, 'remember-me': true };
    if (mfa_code) payload['one-time-password'] = mfa_code;

    const res = await fetch('https://api.tastytrade.com/sessions', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload)
    });

    const json = await res.json();

    // Device/MFA challenge — tell client to ask for code
    if (!res.ok && !json.data?.['session-token']) {
      const errMsg = (json.error?.message || json['error-message'] || '').toLowerCase();
      const needsMfa = errMsg.includes('device') || errMsg.includes('challenge') ||
        errMsg.includes('mfa') || errMsg.includes('verification') || res.status === 401;
      if (needsMfa) {
        return new Response(JSON.stringify({ mfa_required: true }), {
          status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
        });
      }
      return new Response(JSON.stringify({ error: json.error?.message || json['error-message'] || 'Invalid credentials' }), {
        status: 401, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    const rememberToken = json.data?.['remember-token'] || null;

    // Save credentials to Supabase
    await supabase.from('broker_credentials').upsert({
      user_id,
      broker: 'tastytrade',
      credentials: { username, password, remember_token: rememberToken }
    }, { onConflict: 'user_id,broker' });

    return new Response(JSON.stringify({ success: true, username }), {
      status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });

  } catch (err) {
    return new Response(JSON.stringify({ error: String(err) }), {
      status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
});
