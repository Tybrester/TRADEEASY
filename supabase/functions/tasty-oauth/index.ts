import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// Tastytrade Personal Grant flow - use refresh token to get access tokens
const TASTY_TOKEN_URL = 'https://api.tastytrade.com/oauth/token';

async function getAccessToken(refreshToken: string, clientSecret: string): Promise<{ access_token: string; expires_in: number } | null> {
  try {
    const res = await fetch(TASTY_TOKEN_URL, {
      method: 'POST',
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        grant_type: 'refresh_token',
        refresh_token: refreshToken,
        client_secret: clientSecret
      })
    });
    const json = await res.json();
    if (json.error) throw new Error(json.error_description || json.error);
    return { access_token: json.access_token, expires_in: json.expires_in || 900 };
  } catch (err) {
    console.error('[TastyOAuth] Token refresh failed:', err);
    return null;
  }
}

Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: corsHeaders });

  const url = new URL(req.url);
  const action = url.searchParams.get('action');

  // Personal Grant flow: user provides refresh token directly
  if (action === 'connect') {
    const { user_id, refresh_token, client_secret } = await req.json();
    
    if (!user_id || !refresh_token || !client_secret) {
      return new Response(JSON.stringify({ error: 'user_id, refresh_token, and client_secret required' }), {
        status: 400,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    const supabase = createClient(
      Deno.env.get('SUPABASE_URL')!,
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
    );

    try {
      // Get initial access token
      const token = await getAccessToken(refresh_token, client_secret);
      if (!token) throw new Error('Failed to get access token');

      // Get account info
      let accountNumber = null;
      try {
        const acctRes = await fetch('https://api.tastytrade.com/customers/me/accounts', {
          headers: { Authorization: `Bearer ${token.access_token}` }
        });
        const acctJson = await acctRes.json();
        accountNumber = acctJson?.data?.items?.[0]?.account?.['account-number'] || null;
      } catch (_) {}

      // Save credentials (encrypt refresh token in credentials field)
      await supabase.from('broker_credentials').upsert({
        user_id,
        broker: 'tastytrade',
        access_token: token.access_token,
        credentials: { 
          refresh_token,
          client_secret,
          account_number: accountNumber,
          expires_at: new Date(Date.now() + token.expires_in * 1000).toISOString()
        },
        updated_at: new Date().toISOString()
      }, { onConflict: 'user_id,broker' });

      return new Response(JSON.stringify({ success: true, account: accountNumber }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    } catch (err: any) {
      return new Response(JSON.stringify({ error: err.message }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }
  }

  // Refresh access token using stored refresh token
  if (action === 'refresh') {
    const userId = url.searchParams.get('user_id');
    if (!userId) {
      return new Response(JSON.stringify({ error: 'user_id required' }), {
        status: 400,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    const supabase = createClient(
      Deno.env.get('SUPABASE_URL')!,
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
    );

    try {
      const { data: creds } = await supabase.from('broker_credentials')
        .select('credentials')
        .eq('user_id', userId)
        .eq('broker', 'tastytrade')
        .single();
      
      if (!creds?.credentials?.refresh_token) {
        throw new Error('No refresh token found');
      }

      const token = await getAccessToken(creds.credentials.refresh_token, creds.credentials.client_secret);
      if (!token) throw new Error('Token refresh failed');

      // Update access token
      await supabase.from('broker_credentials').update({
        access_token: token.access_token,
        credentials: { ...creds.credentials, expires_at: new Date(Date.now() + token.expires_in * 1000).toISOString() },
        updated_at: new Date().toISOString()
      }).eq('user_id', userId).eq('broker', 'tastytrade');

      return new Response(JSON.stringify({ access_token: token.access_token }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    } catch (err: any) {
      return new Response(JSON.stringify({ error: err.message }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }
  }

  // Get account balance
  if (action === 'balance') {
    const userId = url.searchParams.get('user_id');
    if (!userId) {
      return new Response(JSON.stringify({ error: 'user_id required' }), {
        status: 400,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    const supabase = createClient(
      Deno.env.get('SUPABASE_URL')!,
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
    );

    try {
      const { data: creds } = await supabase.from('broker_credentials')
        .select('credentials')
        .eq('user_id', userId)
        .eq('broker', 'tastytrade')
        .single();
      
      if (!creds?.credentials?.refresh_token) {
        throw new Error('No credentials found');
      }

      // Get fresh access token
      const token = await getAccessToken(creds.credentials.refresh_token, creds.credentials.client_secret);
      if (!token) throw new Error('Token refresh failed');

      // Fetch account balance
      const accountNumber = creds.credentials.account_number;
      const balanceRes = await fetch(`https://api.tastytrade.com/accounts/${accountNumber}/balance`, {
        headers: { Authorization: `Bearer ${token.access_token}` }
      });
      const balanceJson = await balanceRes.json();

      return new Response(JSON.stringify({ 
        balance: balanceJson.data?.['cash-available-to-withdraw'] || balanceJson.data?.['account-value'] || 0,
        account_value: balanceJson.data?.['account-value'] || 0,
        buying_power: balanceJson.data?.['margin-equity'] || balanceJson.data?.['cash-available-to-withdraw'] || 0
      }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    } catch (err: any) {
      return new Response(JSON.stringify({ error: err.message }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }
  }

  // Get option quote (real-time)
  if (action === 'quote') {
    const userId = url.searchParams.get('user_id');
    const symbol = url.searchParams.get('symbol'); // e.g., "SPY" 
    const optionType = url.searchParams.get('type'); // "call" or "put"
    const strike = url.searchParams.get('strike');
    const expiration = url.searchParams.get('expiration'); // YYYY-MM-DD
    
    if (!userId || !symbol || !optionType || !strike || !expiration) {
      return new Response(JSON.stringify({ error: 'user_id, symbol, type, strike, expiration required' }), {
        status: 400,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    const supabase = createClient(
      Deno.env.get('SUPABASE_URL')!,
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
    );

    try {
      const { data: creds } = await supabase.from('broker_credentials')
        .select('credentials')
        .eq('user_id', userId)
        .eq('broker', 'tastytrade')
        .single();
      
      if (!creds?.credentials?.refresh_token) {
        throw new Error('No credentials found');
      }

      // Get fresh access token
      const token = await getAccessToken(creds.credentials.refresh_token, creds.credentials.client_secret);
      if (!token) throw new Error('Token refresh failed');

      // Format option symbol for Tastytrade: SPY 240517 450 C (SPY May 17 2024 $450 Call)
      const expDate = expiration.replace(/-/g, '').slice(2); // YYMMDD
      const strikeNum = Number(strike);
      const optSymbol = `${symbol} ${expDate.slice(0,2)}${expDate.slice(2,4)}${expDate.slice(4)} ${Math.floor(strikeNum)} ${optionType.toLowerCase() === 'call' ? 'C' : 'P'}`;
      
      // Fetch option quote from Tastytrade
      const quoteRes = await fetch(`https://api.tastytrade.com/market-data/quotes?symbol=${encodeURIComponent(optSymbol)}`, {
        headers: { Authorization: `Bearer ${token.access_token}` }
      });
      const quoteJson = await quoteRes.json();
      
      const quote = quoteJson.data?.items?.[0];
      if (!quote) throw new Error('No quote returned');

      return new Response(JSON.stringify({
        bid: quote.bid || 0,
        ask: quote.ask || 0,
        last: quote.last || 0,
        mid: (quote.bid + quote.ask) / 2 || quote.last || 0,
        volume: quote.volume || 0,
        open_interest: quote['open-interest'] || 0,
        source: 'tastytrade'
      }), {
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    } catch (err: any) {
      return new Response(JSON.stringify({ error: err.message }), {
        status: 500,
        headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }
  }

  return new Response(JSON.stringify({ error: 'Invalid action. Use: connect, refresh, balance, or quote' }), {
    status: 400,
    headers: { ...corsHeaders, 'Content-Type': 'application/json' }
  });
});
