import { createClient } from 'https://esm.sh/@supabase/supabase-js@2';

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
};

// ─────────────────────────────────────────────
// MATH HELPERS
// ─────────────────────────────────────────────

function calcEMA(data: number[], period: number): number[] {
  const k = 2 / (period + 1);
  const ema = new Array(data.length).fill(0);
  ema[0] = data[0];
  for (let i = 1; i < data.length; i++) ema[i] = data[i] * k + ema[i - 1] * (1 - k);
  return ema;
}

function calcATR(highs: number[], lows: number[], closes: number[], period: number): number[] {
  const tr = highs.map((h, i) => i === 0 ? h - lows[i] : Math.max(h - lows[i], Math.abs(h - closes[i - 1]), Math.abs(lows[i] - closes[i - 1])));
  const atr = new Array(tr.length).fill(0);
  atr[period - 1] = tr.slice(0, period).reduce((a, b) => a + b) / period;
  for (let i = period; i < tr.length; i++) atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period;
  return atr;
}

function calcSuperTrend(highs: number[], lows: number[], closes: number[], atrLen: number, mult: number) {
  const atr = calcATR(highs, lows, closes, atrLen);
  const n = closes.length;
  const trend = new Array(n).fill(1);
  const upperBand = new Array(n).fill(0);
  const lowerBand = new Array(n).fill(0);
  for (let i = 0; i < n; i++) {
    const hl2 = (highs[i] + lows[i]) / 2;
    upperBand[i] = hl2 + mult * atr[i];
    lowerBand[i] = hl2 - mult * atr[i];
    if (i > 0) {
      lowerBand[i] = lowerBand[i] > lowerBand[i - 1] || closes[i - 1] < lowerBand[i - 1] ? lowerBand[i] : lowerBand[i - 1];
      upperBand[i] = upperBand[i] < upperBand[i - 1] || closes[i - 1] > upperBand[i - 1] ? upperBand[i] : upperBand[i - 1];
      if (trend[i - 1] === -1 && closes[i] > upperBand[i - 1]) trend[i] = 1;
      else if (trend[i - 1] === 1 && closes[i] < lowerBand[i - 1]) trend[i] = -1;
      else trend[i] = trend[i - 1];
    }
  }
  return { trend, upperBand, lowerBand };
}

function calcDMI(highs: number[], lows: number[], closes: number[], period: number) {
  const n = highs.length;
  const plusDM = new Array(n).fill(0);
  const minusDM = new Array(n).fill(0);
  for (let i = 1; i < n; i++) {
    const up = highs[i] - highs[i - 1];
    const down = lows[i - 1] - lows[i];
    if (up > down && up > 0) plusDM[i] = up;
    if (down > up && down > 0) minusDM[i] = down;
  }
  const atr = calcATR(highs, lows, closes, period);
  const smoothPlusDM = calcEMA(plusDM, period);
  const smoothMinusDM = calcEMA(minusDM, period);
  const plusDI = smoothPlusDM.map((v, i) => atr[i] ? (v / atr[i]) * 100 : 0);
  const minusDI = smoothMinusDM.map((v, i) => atr[i] ? (v / atr[i]) * 100 : 0);
  const dx = plusDI.map((v, i) => (v + minusDI[i]) ? Math.abs(v - minusDI[i]) / (v + minusDI[i]) * 100 : 0);
  const adx = new Array(n).fill(0);
  const start2 = period * 2 - 1;
  if (start2 < n) {
    const validDx = dx.slice(period - 1, start2);
    adx[start2] = validDx.reduce((a, b) => a + b, 0) / period;
    for (let i = start2 + 1; i < n; i++) adx[i] = (adx[i - 1] * (period - 1) + dx[i]) / period;
  }
  return { plusDI, minusDI, adx };
}

function calcRSI(closes: number[], period: number): number[] {
  const rsi: number[] = new Array(closes.length).fill(NaN);
  if (closes.length < period + 1) return rsi;
  let gains = 0, losses = 0;
  for (let i = 1; i <= period; i++) {
    const diff = closes[i] - closes[i - 1];
    if (diff > 0) gains += diff; else losses -= diff;
  }
  let avgGain = gains / period, avgLoss = losses / period;
  rsi[period] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
  for (let i = period + 1; i < closes.length; i++) {
    const diff = closes[i] - closes[i - 1];
    const g = diff > 0 ? diff : 0, l = diff < 0 ? -diff : 0;
    avgGain = (avgGain * (period - 1) + g) / period;
    avgLoss = (avgLoss * (period - 1) + l) / period;
    rsi[i] = avgLoss === 0 ? 100 : 100 - 100 / (1 + avgGain / avgLoss);
  }
  return rsi;
}

function calcMACD(closes: number[], fast: number, slow: number, signal: number): { macdLine: number[], signalLine: number[], hist: number[] } {
  const emaFast = calcEMA(closes, fast);
  const emaSlow = calcEMA(closes, slow);
  const macdLine = closes.map((_, i) => (isNaN(emaFast[i]) || isNaN(emaSlow[i])) ? NaN : emaFast[i] - emaSlow[i]);
  const validStart = macdLine.findIndex(v => !isNaN(v));
  const signalLine: number[] = new Array(closes.length).fill(NaN);
  if (validStart >= 0) {
    const emaSignal = calcEMA(macdLine.slice(validStart), signal);
    for (let i = 0; i < emaSignal.length; i++) signalLine[validStart + i] = emaSignal[i];
  }
  const hist = macdLine.map((v, i) => (isNaN(v) || isNaN(signalLine[i])) ? NaN : v - signalLine[i]);
  return { macdLine, signalLine, hist };
}

function generateSignalRSIMACD(candles: Candle[], tradeDirection = 'both'): { signal: 'buy' | 'sell' | 'none', price: number, trend: number, ema: number, adx: number, reason: string } {
  const closes = candles.map(c => c.close);
  const n = closes.length;
  const i = n - 2;
  const rsi = calcRSI(closes, 14);
  const ema50 = calcEMA(closes, 50);
  const { hist } = calcMACD(closes, 12, 26, 9);
  const curRSI = rsi[i], curEma = ema50[i], curHist = hist[i], curClose = closes[i];
  
  // Replay position state
  let inLong = false, inShort = false;
  for (let j = 50; j < i; j++) {
    const r = rsi[j], h = hist[j], e = ema50[j], c = closes[j];
    if (isNaN(r) || isNaN(h) || isNaN(e)) continue;
    const buyCond = (r < 30 || h > 0) && c > e;
    const sellCond = (r > 70 || h < 0) && c < e;
    if (!inLong && !inShort && buyCond) inLong = true;
    else if (!inLong && !inShort && sellCond) inShort = true;
    else if (inLong && sellCond) { inLong = false; inShort = true; }
    else if (inShort && buyCond) { inShort = false; inLong = true; }
  }
  
  const buyCond  = (curRSI < 30 || curHist > 0) && curClose > curEma;
  const sellCond = (curRSI > 70 || curHist < 0) && curClose < curEma;
  let signal: 'buy' | 'sell' | 'none' = 'none';
  let reason = `rsi=${curRSI?.toFixed(1)}, macd_hist=${curHist?.toFixed(4)}, ema=${curEma?.toFixed(2)}, close=${curClose?.toFixed(2)}, pos=${inLong ? 'long' : inShort ? 'short' : 'flat'}`;
  
  if (buyCond) {
    if (inShort) { signal = 'buy'; reason = `EXIT SHORT->LONG. ${reason}`; }
    else if (!inLong) { signal = 'buy'; reason = `ENTER LONG. ${reason}`; }
  } else if (sellCond) {
    if (inLong) { signal = 'sell'; reason = `EXIT LONG->SHORT. ${reason}`; }
    else if (!inShort && tradeDirection !== 'long') { signal = 'sell'; reason = `ENTER SHORT. ${reason}`; }
    else if (tradeDirection === 'long' && inLong) { signal = 'sell'; reason = `EXIT LONG (long-only). ${reason}`; }
  }
  return { signal, price: curClose, trend: buyCond ? 1 : -1, ema: curEma, adx: curRSI, reason };
}

// ─────────────────────────────────────────────
// BOOF 2.0 ML-STYLE INDICATOR
// ─────────────────────────────────────────────

function generateSignalBoof20(candles: Candle[], tradeDirection = 'both', thresholdBuy = 0.0, thresholdSell = 0.0): { signal: 'buy' | 'sell' | 'none', price: number, trend: number, ema: number, adx: number, reason: string } {
  const highs = candles.map(c => c.high);
  const lows = candles.map(c => c.low);
  const closes = candles.map(c => c.close);
  const n = closes.length;

  if (n < 25) {
    return { signal: 'none', price: closes[n - 1], trend: 0, ema: closes[n - 1], adx: 50, reason: 'Insufficient data for Boof 2.0' };
  }

  const length = 14, maFast = 5, maSlow = 20;

  // Past return
  const pastReturn: number[] = new Array(n).fill(0);
  for (let i = length; i < n; i++) {
    pastReturn[i] = (closes[i] - closes[i - length]) / closes[i - length];
  }

  // MA calculations
  const maFastVals: number[] = new Array(n).fill(NaN);
  const maSlowVals: number[] = new Array(n).fill(NaN);
  for (let i = maFast - 1; i < n; i++) {
    maFastVals[i] = closes.slice(i - maFast + 1, i + 1).reduce((a, b) => a + b, 0) / maFast;
  }
  for (let i = maSlow - 1; i < n; i++) {
    maSlowVals[i] = closes.slice(i - maSlow + 1, i + 1).reduce((a, b) => a + b, 0) / maSlow;
  }

  // RSI
  const rsi = calcRSI(closes, length);

  // Current bar
  const i = n - 2;
  const rPast = pastReturn[i] || 0;
  const rMa = (maFastVals[i] - maSlowVals[i]) / closes[i] || 0;
  const rRsi = (rsi[i] - 50) / 50 || 0;

  // Simplified ATR
  const atrSlice = highs.slice(i - 13, i + 1).map((h, idx) => h - lows[i - 13 + idx]);
  const rAtr = Math.max(...atrSlice) / closes[i] || 0;

  // ML prediction
  const predictedReturn = 0.4 * rPast + 0.3 * rMa + 0.2 * rRsi - 0.1 * rAtr;

  let signal: 'buy' | 'sell' | 'none' = 'none';
  let reason = `predicted=${predictedReturn.toFixed(4)}, rsi=${rsi[i]?.toFixed(1)}`;

  if (predictedReturn > thresholdBuy) {
    signal = 'buy';
    reason = `Boof 2.0 BUY. ${reason}`;
  } else if (predictedReturn < thresholdSell) {
    signal = 'sell';
    reason = `Boof 2.0 SELL. ${reason}`;
  }

  if (tradeDirection === 'long' && signal === 'sell') signal = 'none';
  if (tradeDirection === 'short' && signal === 'buy') signal = 'none';

  return { signal, price: closes[i], trend: predictedReturn > 0 ? 1 : -1, ema: maSlowVals[i], adx: rsi[i], reason };
}

// ─────────────────────────────────────────────
// BOOF 3.0 KMEANS REGIME DETECTION
// ─────────────────────────────────────────────

type MarketRegime = 'Trend' | 'Range' | 'HighVol';

function kMeansClustering(data: number[][], k: number, maxIterations = 100) {
  const n = data.length;
  const dims = data[0].length;
  const centroids: number[][] = [];
  const usedIndices = new Set<number>();
  for (let i = 0; i < k; i++) {
    let idx = Math.floor(Math.random() * n);
    while (usedIndices.has(idx)) idx = Math.floor(Math.random() * n);
    usedIndices.add(idx);
    centroids.push([...data[idx]]);
  }
  let clusters: number[] = new Array(n).fill(0);
  let changed = true, iterations = 0;
  while (changed && iterations < maxIterations) {
    changed = false;
    iterations++;
    for (let i = 0; i < n; i++) {
      let minDist = Infinity, bestCluster = 0;
      for (let j = 0; j < k; j++) {
        let dist = 0;
        for (let d = 0; d < dims; d++) dist += (data[i][d] - centroids[j][d]) ** 2;
        dist = Math.sqrt(dist);
        if (dist < minDist) { minDist = dist; bestCluster = j; }
      }
      if (clusters[i] !== bestCluster) { clusters[i] = bestCluster; changed = true; }
    }
    const newCentroids: number[][] = Array(k).fill(null).map(() => Array(dims).fill(0));
    const counts = Array(k).fill(0);
    for (let i = 0; i < n; i++) {
      const c = clusters[i];
      counts[c]++;
      for (let d = 0; d < dims; d++) newCentroids[c][d] += data[i][d];
    }
    for (let j = 0; j < k; j++) {
      if (counts[j] > 0) {
        for (let d = 0; d < dims; d++) newCentroids[j][d] /= counts[j];
        centroids[j] = newCentroids[j];
      }
    }
  }
  return { clusters, centroids };
}

function generateSignalBoof30(candles: Candle[], tradeDirection = 'both'): { signal: 'buy' | 'sell' | 'none', price: number, trend: number, ema: number, adx: number, reason: string } {
  const highs = candles.map(c => c.high);
  const lows = candles.map(c => c.low);
  const closes = candles.map(c => c.close);
  const volumes = candles.map(c => (c as any).volume || 1000000);
  const n = closes.length;

  if (n < 35) return { signal: 'none', price: closes[n - 1], trend: 0, ema: closes[n - 1], adx: 50, reason: 'Insufficient data' };

  const lookback = 14;

  // Returns
  const returns: number[] = new Array(n).fill(0);
  for (let i = 1; i < n; i++) returns[i] = (closes[i] - closes[i - 1]) / closes[i - 1];

  // Return std
  const returnStd: number[] = new Array(n).fill(0);
  for (let i = lookback; i < n; i++) {
    const slice = returns.slice(i - lookback + 1, i + 1);
    const mean = slice.reduce((a, b) => a + b, 0) / lookback;
    returnStd[i] = Math.sqrt(slice.reduce((a, b) => a + (b - mean) ** 2, 0) / lookback);
  }

  // MA slope
  const maFast: number[] = new Array(n).fill(NaN);
  const maSlow: number[] = new Array(n).fill(NaN);
  for (let i = 4; i < n; i++) maFast[i] = closes.slice(i - 4, i + 1).reduce((a, b) => a + b, 0) / 5;
  for (let i = 19; i < n; i++) maSlow[i] = closes.slice(i - 19, i + 1).reduce((a, b) => a + b, 0) / 20;
  const maSlope = maFast.map((f, i) => !isNaN(f) && !isNaN(maSlow[i]) ? f - maSlow[i] : 0);

  // RSI
  const rsi = calcRSI(closes, lookback);

  // Volume std
  const volumeStd: number[] = new Array(n).fill(0);
  for (let i = lookback; i < n; i++) {
    const slice = volumes.slice(i - lookback + 1, i + 1);
    const mean = slice.reduce((a, b) => a + b, 0) / lookback;
    volumeStd[i] = Math.sqrt(slice.reduce((a, b) => a + (b - mean) ** 2, 0) / lookback);
  }

  // Prepare features for clustering
  const validStart = Math.max(lookback, 20);
  const features: number[][] = [];
  const validIndices: number[] = [];
  for (let i = validStart; i < n; i++) {
    if (!isNaN(rsi[i])) {
      features.push([returnStd[i] * 100, maSlope[i], rsi[i], volumeStd[i] / 1000000]);
      validIndices.push(i);
    }
  }

  if (features.length < 10) return { signal: 'none', price: closes[n - 1], trend: 0, ema: closes[n - 1], adx: 50, reason: 'Not enough data' };

  // KMeans clustering
  const { clusters } = kMeansClustering(features, 3, 50);

  // Map clusters to regimes by avg slope
  const clusterStats: { cluster: number, avgSlope: number }[] = [];
  for (let c = 0; c < 3; c++) {
    const indices = validIndices.filter((_, idx) => clusters[idx] === c);
    const avgSlope = indices.reduce((a, idx) => a + maSlope[idx], 0) / indices.length;
    clusterStats.push({ cluster: c, avgSlope });
  }
  clusterStats.sort((a, b) => a.avgSlope - b.avgSlope);
  const regimeMap: Record<number, MarketRegime> = {
    [clusterStats[0].cluster]: 'Range',
    [clusterStats[1].cluster]: 'HighVol',
    [clusterStats[2].cluster]: 'Trend'
  };

  // Generate signals for each point
  const signals: { regime: MarketRegime, signal: number }[] = [];
  for (let idx = 0; idx < validIndices.length; idx++) {
    const i = validIndices[idx];
    const regime = regimeMap[clusters[idx]];
    let signal = 0;
    if (regime === 'Trend') {
      if (maSlope[i] > 0 && rsi[i] > 50) signal = 1;
      else if (maSlope[i] < 0 && rsi[i] < 50) signal = -1;
    } else if (regime === 'Range') {
      if (rsi[i] < 35) signal = 1;
      else if (rsi[i] > 65) signal = -1;
    } else if (regime === 'HighVol') {
      if (rsi[i] < 25 && maSlope[i] > 0) signal = 1;
      else if (rsi[i] > 75 && maSlope[i] < 0) signal = -1;
    }
    signals.push({ regime, signal });
  }

  // Current bar - independent signal (no position tracking for options)
  const i = n - 2;
  const idx = validIndices.indexOf(i);
  const current = idx >= 0 ? signals[idx] : { regime: 'Range' as MarketRegime, signal: 0 };
  const curClose = closes[i];

  let signal: 'buy' | 'sell' | 'none' = 'none';
  let reason = `regime=${current.regime}, rsi=${rsi[i]?.toFixed(1)}, slope=${maSlope[i]?.toFixed(4)}`;

  if (current.signal === 1) {
    signal = 'buy';
    reason = `Boof 3.0 BUY [${current.regime}]. ${reason}`;
  } else if (current.signal === -1) {
    signal = 'sell';
    reason = `Boof 3.0 SELL [${current.regime}]. ${reason}`;
  } else {
    reason = `Boof 3.0 NONE [${current.regime}]. ${reason}`;
  }

  if (tradeDirection === 'long' && signal === 'sell') signal = 'none';
  if (tradeDirection === 'short' && signal === 'buy') signal = 'none';

  return { signal, price: curClose, trend: maSlope[i] > 0 ? 1 : -1, ema: maSlow[i], adx: rsi[i], reason };
}

// ─────────────────────────────────────────────
// FETCH CANDLES (Yahoo Finance - Free)
// ─────────────────────────────────────────────

interface Candle { time: number; open: number; high: number; low: number; close: number; }

async function fetchAlpacaSpotPrice(symbol: string, api_key: string, secret_key: string): Promise<number | null> {
  try {
    const res = await fetch(`https://data.alpaca.markets/v2/stocks/${symbol}/quotes/latest`, {
      headers: { 'APCA-API-KEY-ID': api_key, 'APCA-API-SECRET-KEY': secret_key }
    });
    const json = await res.json();
    const ask = json?.quote?.ap;
    const bid = json?.quote?.bp;
    if (ask > 0 && bid > 0) {
      const mid = (ask + bid) / 2;
      console.log(`[OptionsBot] Alpaca spot ${symbol} = $${mid.toFixed(2)} (bid=$${bid} ask=$${ask})`);
      return mid;
    }
  } catch (_) {}
  return null;
}

async function fetchPolygonSpotPrice(symbol: string): Promise<number | null> {
  try {
    const polygonKey = Deno.env.get('POLYGON_API_KEY');
    if (!polygonKey) return null;
    const url = `https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/${symbol}?apiKey=${polygonKey}`;
    const res = await fetch(url);
    const json = await res.json();
    const ticker = json?.ticker;
    const p = ticker?.lastTrade?.p || ticker?.lastQuote?.P || ticker?.prevDay?.c;
    if (p && p > 0) {
      console.log(`[OptionsBot] Polygon stock spot ${symbol} = $${p} (delayed)`);
      return p;
    }
  } catch (_) {}
  return null;
}

// Known reasonable price ranges for sanity checking spot prices
const SPOT_SANITY: Record<string, [number, number]> = {
  'SPY':  [400, 700], 'QQQ':  [300, 600], 'IWM':  [150, 350],
  'AAPL': [100, 400], 'MSFT': [200, 600], 'NVDA': [50,  200],
  'TSLA': [100, 500], 'AMZN': [100, 300], 'META': [200, 800],
  'GOOG': [100, 300], 'GOOGL':[100, 300],
};

function sanityCheckSpot(symbol: string, price: number): boolean {
  const bounds = SPOT_SANITY[symbol];
  if (!bounds) return price > 0 && price < 10000; // generic: just must be positive and sane
  const ok = price >= bounds[0] && price <= bounds[1];
  if (!ok) console.log(`[OptionsBot] SANITY FAIL: ${symbol} spot $${price} outside expected range $${bounds[0]}-$${bounds[1]}`);
  return ok;
}

async function fetchTastytradeSpotPrice(symbol: string, accessToken: string): Promise<number | null> {
  try {
    // Tastytrade equity quotes endpoint
    const res = await fetch(`https://api.tastytrade.com/market-data/quotes?symbols[]=${encodeURIComponent(symbol)}`, {
      headers: { Authorization: `Bearer ${accessToken}` }
    });
    const json = await res.json();
    const quote = json?.data?.items?.[0];
    const mid = quote?.mid || ((Number(quote?.bid) + Number(quote?.ask)) / 2) || quote?.last;
    if (mid && mid > 0 && sanityCheckSpot(symbol, mid)) {
      console.log(`[OptionsBot] Tastytrade real-time spot ${symbol} = $${mid} (bid=$${quote?.bid} ask=$${quote?.ask})`);
      return mid;
    }
    console.log(`[OptionsBot] Tastytrade spot bad/missing for ${symbol}: mid=${mid} raw=${JSON.stringify(quote)}`);
  } catch (err) {
    console.log(`[OptionsBot] Tastytrade spot fetch failed for ${symbol}:`, err);
  }
  return null;
}

async function fetchSpotPrice(symbol: string, alpacaApiKey?: string, alpacaSecretKey?: string): Promise<number | null> {
  // For paper trading: use real-time Alpaca data for accurate backtesting
  if (alpacaApiKey && alpacaSecretKey) {
    const p = await fetchAlpacaSpotPrice(symbol, alpacaApiKey, alpacaSecretKey);
    if (p) return p;
  }
  // Fallback: Yahoo real-time (better than delayed for paper testing)
  try {
    const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=1m&range=1d`;
    const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' } });
    const json = await res.json();
    const meta = json?.chart?.result?.[0]?.meta;
    const p = meta?.regularMarketPrice ?? meta?.price;
    if (p && p > 0) { console.log(`[OptionsBot] Yahoo spot ${symbol} = $${p}`); return p; }
  } catch (_) {}
  // Last resort: Polygon delayed
  return await fetchPolygonSpotPrice(symbol);
}

async function fetchCandles(symbol: string, interval = '1h', bars = 150): Promise<Candle[]> {
  // Yahoo Finance API (free, no key needed)
  const intervalMap: Record<string, { yahooInterval: string; range: string }> = {
    '1m':  { yahooInterval: '1m',  range: '5d'   },
    '5m':  { yahooInterval: '5m',  range: '5d'   },
    '10m': { yahooInterval: '15m', range: '5d'   },
    '15m': { yahooInterval: '15m', range: '5d'   },
    '30m': { yahooInterval: '30m', range: '1mo'  },
    '45m': { yahooInterval: '60m', range: '1mo'  },
    '1h':  { yahooInterval: '60m', range: '1mo'  },
    '2h':  { yahooInterval: '60m', range: '3mo'  },
    '4h':  { yahooInterval: '60m', range: '6mo'  },
    '1d':  { yahooInterval: '1d',  range: '1y'   },
  };
  const { yahooInterval, range } = intervalMap[interval] ?? intervalMap['1h'];
  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${symbol}?interval=${yahooInterval}&range=${range}`;
  const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36' } });
  if (!res.ok) throw new Error(`Yahoo API error: ${res.status}`);
  const json = await res.json();
  if (!json.chart?.result?.[0]) throw new Error(`No Yahoo data for ${symbol}`);
  const result = json.chart.result[0];
  const timestamps = result.timestamp || [];
  const quote = result.indicators?.quote?.[0] || {};
  const candles: Candle[] = [];
  for (let i = 0; i < timestamps.length; i++) {
    if (quote.open?.[i] && quote.high?.[i] && quote.low?.[i] && quote.close?.[i]) {
      candles.push({ time: timestamps[i] * 1000, open: quote.open[i], high: quote.high[i], low: quote.low[i], close: quote.close[i] });
    }
  }
  if (candles.length < 60) throw new Error(`Not enough data for ${symbol} (got ${candles.length} candles)`);
  return candles.slice(-bars);
}

// ─────────────────────────────────────────────
// SIGNAL GENERATION
// ─────────────────────────────────────────────

function generateSignal(candles: Candle[], settings: BotSettings): { signal: 'buy' | 'sell' | 'none', price: number, trend: number, ema: number, adx: number, reason: string } {
  const highs  = candles.map(c => c.high);
  const lows   = candles.map(c => c.low);
  const closes = candles.map(c => c.close);
  const n = closes.length;
  const tradeDirection = settings.tradeDirection || 'both';
  const emaArr = calcEMA(closes, settings.emaLength);
  const { trend } = calcSuperTrend(highs, lows, closes, settings.atrLength, settings.atrMultiplier);
  const { adx }   = calcDMI(highs, lows, closes, settings.adxLength);
  
  // Options bot: no position state replay needed - each contract is independent
  
  const i = n - 2;
  const curTrend = trend[i], prevTrend = trend[i - 1];
  const curEma = emaArr[i], curAdx = adx[i], curClose = closes[i];
  const trendJustFlipped = curTrend !== prevTrend;
  const longOK  = curTrend === 1  && curClose > curEma && curAdx > settings.adxThreshold;
  const shortOK = curTrend === -1 && curClose < curEma && curAdx > settings.adxThreshold;
  let signal: 'buy' | 'sell' | 'none' = 'none';
  let reason = `trend=${curTrend}, close=${curClose.toFixed(2)}, ema=${curEma.toFixed(2)}, adx=${curAdx?.toFixed(1)}`;
  // Fire on current trend conditions - no position state (each options contract is independent)
  if (longOK) {
    signal = 'buy';
    reason = `${trendJustFlipped ? 'TREND FLIP ' : ''}ENTER LONG. SuperTrend UP. ${reason}`;
  } else if (shortOK && tradeDirection !== 'long') {
    signal = 'sell';
    reason = `${trendJustFlipped ? 'TREND FLIP ' : ''}ENTER SHORT. SuperTrend DOWN. ${reason}`;
  }
  return { signal, price: curClose, trend: curTrend, ema: curEma, adx: curAdx, reason };
}

// ─────────────────────────────────────────────
// BLACK-SCHOLES OPTION PRICING
// ─────────────────────────────────────────────

function erf(x: number): number {
  const a1=0.254829592,a2=-0.284496736,a3=1.421413741,a4=-1.453152027,a5=1.061405429,p=0.3275911;
  const sign = x < 0 ? -1 : 1;
  x = Math.abs(x);
  const t = 1 / (1 + p * x);
  const y = 1 - (((((a5*t+a4)*t)+a3)*t+a2)*t+a1)*t*Math.exp(-x*x);
  return sign * y;
}

function normCDF(x: number): number { return 0.5 * (1 + erf(x / Math.sqrt(2))); }

function blackScholes(S: number, K: number, T: number, r: number, sigma: number, type: 'call' | 'put'): number {
  if (T <= 0) return Math.max(0, type === 'call' ? S - K : K - S);
  const d1 = (Math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * Math.sqrt(T));
  const d2 = d1 - sigma * Math.sqrt(T);
  if (type === 'call') return S * normCDF(d1) - K * Math.exp(-r * T) * normCDF(d2);
  return K * Math.exp(-r * T) * normCDF(-d2) - S * normCDF(-d1);
}

function calcHistoricalVolatility(closes: number[], period = 20, interval = '1d'): number {
  const returns: number[] = [];
  for (let i = 1; i < closes.length; i++) returns.push(Math.log(closes[i] / closes[i - 1]));
  const recent = returns.slice(-period);
  const mean = recent.reduce((a, b) => a + b, 0) / recent.length;
  const variance = recent.reduce((a, b) => a + Math.pow(b - mean, 2), 0) / recent.length;
  // Annualization factor: scale per-bar variance to annual
  const barsPerDay: Record<string, number> = { '1m': 390, '5m': 78, '10m': 39, '15m': 26, '30m': 13, '45m': 9, '1h': 7, '2h': 4, '4h': 2, '1d': 1 };
  const bpd = barsPerDay[interval] ?? 1;
  return Math.sqrt(variance * 252 * bpd);
}

// ─────────────────────────────────────────────
// OPTION PRICE via Polygon.io real options chain
// ─────────────────────────────────────────────

async function fetchRealOptionPrice(symbol: string, strike: number, expiration: string, optionType: string, interval = '1h', userId?: string): Promise<number> {
  // Try Tastytrade first for real-time data (if user connected)
  if (userId) {
    try {
      const supabase = createClient(
        Deno.env.get('SUPABASE_URL')!,
        Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
      );
      
      const { data: creds } = await supabase.from('broker_credentials')
        .select('credentials')
        .eq('user_id', userId)
        .eq('broker', 'tastytrade')
        .maybeSingle();
      
      if (creds?.credentials?.refresh_token) {
        // Get fresh access token
        const tokenRes = await fetch(`${Deno.env.get('SUPABASE_URL')}/functions/v1/tasty-oauth?action=refresh&user_id=${userId}`, {
          headers: { 'Authorization': `Bearer ${Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')}` }
        });
        const tokenJson = await tokenRes.json();
        
        if (tokenJson.access_token) {
          // Format option symbol for Tastytrade
          const expDate = expiration.replace(/-/g, '').slice(2); // YYMMDD
          const optSymbol = `${symbol} ${expDate.slice(0,2)}${expDate.slice(2,4)}${expDate.slice(4)} ${Math.floor(strike)} ${optionType.toLowerCase() === 'call' ? 'C' : 'P'}`;
          
          const quoteRes = await fetch(`https://api.tastytrade.com/market-data/quotes?symbol=${encodeURIComponent(optSymbol)}`, {
            headers: { Authorization: `Bearer ${tokenJson.access_token}` }
          });
          const quoteJson = await quoteRes.json();
          const quote = quoteJson.data?.items?.[0];
          
          if (quote?.mid || quote?.last) {
            const price = quote.mid || quote.last;
            console.log(`[OptionsBot] Tastytrade real-time price for ${optSymbol}: $${price} (bid=$${quote.bid} ask=$${quote.ask})`);
            return price;
          }
        }
      }
    } catch (err) {
      console.log('[OptionsBot] Tastytrade price fetch failed:', err);
    }
  }
  
  // Fallback to Polygon (15-min delayed)
  try {
    const polygonKey = Deno.env.get('POLYGON_API_KEY');
    if (polygonKey) {
      // Format: O:SPY260505C00725000
      const exp = expiration.replace(/-/g, '').slice(2); // YYMMDD
      const strikeStr = String(Math.round(strike * 1000)).padStart(8, '0');
      const typeChar = optionType.toLowerCase() === 'call' ? 'C' : 'P';
      const ticker = `O:${symbol}${exp}${typeChar}${strikeStr}`;
      // Use direct single-contract snapshot endpoint
      const snapUrl = `https://api.polygon.io/v3/snapshot/options/${symbol}/${ticker}?apiKey=${polygonKey}`;
      const snapRes = await fetch(snapUrl);
      const snapJson = await snapRes.json();
      console.log(`[OptionsBot] Polygon snapshot ${ticker}: status=${snapJson?.status} error=${snapJson?.error} message=${snapJson?.message} hasResults=${!!snapJson?.results}`);
      const r = snapJson?.results;
      // last_trade.price is most recent (15-min delayed); day.vwap is intraday avg; day.close is yesterday's close
      const bestPrice = r?.last_trade?.price || r?.day?.vwap || r?.day?.close;
      if (bestPrice && bestPrice > 0) {
        console.log(`[OptionsBot] Polygon snapshot price for ${ticker}: $${bestPrice} (last_trade=$${r?.last_trade?.price} day_close=$${r?.day?.close})`);
        return bestPrice;
      }
    }
  } catch (err) {
    console.log('[OptionsBot] Polygon price fetch failed:', err);
  }
  // Black-Scholes with realistic IV (historical vol tends to overprice, use market-calibrated IV)
  try {
    const candles = await fetchCandles(symbol, interval, 60);
    if (!candles.length) return 0;
    const spotPrice = candles[candles.length - 1].close;
    // Use realistic implied vol: ETFs ~15%, large caps ~25%, small caps/volatile ~40%
    const etfs = ['SPY','QQQ','IWM','DIA','GLD','TLT','XLF','XLE','XLK','XLV','EEM','VXX'];
    const highVol = ['TSLA','NVDA','AMD','MSTR','COIN','PLTR','GME','AMC','RIVN','LCID'];
    const iv = etfs.includes(symbol) ? 0.15 : highVol.includes(symbol) ? 0.45 : 0.25;
    // Use 4PM ET (20:00 UTC) on expiry date as expiry time — not midnight
    const expParts = expiration.split('-');
    const expDate = new Date(Date.UTC(Number(expParts[0]), Number(expParts[1]) - 1, Number(expParts[2]), 20, 0, 0));
    const T = Math.max(1 / (365 * 24 * 60), (expDate.getTime() - Date.now()) / (365 * 24 * 60 * 60 * 1000));
    const price = blackScholes(spotPrice, strike, T, 0.05, iv, optionType as 'call' | 'put');
    console.log(`[OptionsBot] BS price for ${symbol} ${optionType} $${strike} (IV=${(iv*100).toFixed(0)}% T=${(T*365*24).toFixed(1)}h): $${price.toFixed(4)}`);
    return price;
  } catch (_) { return 0; }
}

function getExpirationDate(type: string): string {
  const now = new Date();
  if (type === '0dte') {
    // Find the closest future valid expiration day (today if available, otherwise next available day)
    const target = new Date(now.getTime());
    
    // Search up to 7 days forward for the next valid trading day
    for (let i = 0; i < 7; i++) {
      const candidate = new Date(target.getTime());
      candidate.setDate(candidate.getDate() + i);
      const day = candidate.getDay();
      
      // Skip weekends (0=Sunday, 6=Saturday)
      if (day === 0 || day === 6) continue;
      
      // Return first valid weekday (handles holidays via findValidExpiration later)
      return candidate.toISOString().split('T')[0];
    }
    
    // Fallback to today if no valid day found (shouldn't happen)
    return target.toISOString().split('T')[0];
  } else if (type === 'weekly') {
    // Always pick NEXT Friday for consistent 7+ day holds (minimum 7 days)
    const thisFriday = new Date(now.getTime());
    const daysToThisFriday = (5 - thisFriday.getDay() + 7) % 7;
    thisFriday.setDate(thisFriday.getDate() + daysToThisFriday);
    
    const nextFriday = new Date(thisFriday.getTime());
    nextFriday.setDate(nextFriday.getDate() + 7);
    
    // Always use next Friday (at least 7 days from today)
    return nextFriday.toISOString().split('T')[0];
  } else if (type === 'biweekly') {
    // Biweekly — closest Friday to 14 days from now (could be 13, 14, or 15 days out)
    const target = new Date(now.getTime());
    target.setDate(target.getDate() + 14);
    const dow = target.getDay();
    const fwdDays = (5 - dow + 7) % 7;           // days forward to reach Friday
    const bkDays = dow === 5 ? 0 : (dow - 5 + 7) % 7; // days back to reach Friday
    const closestFri = new Date(target.getTime());
    closestFri.setDate(target.getDate() + (fwdDays <= bkDays ? fwdDays : -bkDays));
    return closestFri.toISOString().split('T')[0];
  } else {
    // Monthly — third Friday closest to 30 days away
    // Find this month's and next month's third Friday
    const thisMonth = new Date(now.getFullYear(), now.getMonth(), 1);
    let thisFridays = 0, thisThirdFriday: Date | null = null;
    for (let d = 1; d <= 31; d++) {
      const date = new Date(thisMonth.getFullYear(), thisMonth.getMonth(), d);
      if (date.getMonth() !== thisMonth.getMonth()) break;
      if (date.getDay() === 5) {
        thisFridays++;
        if (thisFridays === 3) { thisThirdFriday = date; break; }
      }
    }
    
    const nextMonth = new Date(now.getFullYear(), now.getMonth() + 1, 1);
    let nextFridays = 0, nextThirdFriday: Date | null = null;
    for (let d = 1; d <= 31; d++) {
      const date = new Date(nextMonth.getFullYear(), nextMonth.getMonth(), d);
      if (date.getMonth() !== nextMonth.getMonth()) break;
      if (date.getDay() === 5) {
        nextFridays++;
        if (nextFridays === 3) { nextThirdFriday = date; break; }
      }
    }
    
    // Pick whichever third Friday is closest to 30 days from now
    const daysToThis = thisThirdFriday ? Math.ceil((thisThirdFriday.getTime() - now.getTime()) / (24 * 60 * 60 * 1000)) : Infinity;
    const daysToNext = nextThirdFriday ? Math.ceil((nextThirdFriday.getTime() - now.getTime()) / (24 * 60 * 60 * 1000)) : Infinity;
    
    const diffFrom30This = Math.abs(daysToThis - 30);
    const diffFrom30Next = Math.abs(daysToNext - 30);
    
    const target = diffFrom30This <= diffFrom30Next && daysToThis > 0 ? thisThirdFriday : nextThirdFriday;
    return target ? target.toISOString().split('T')[0] : (thisThirdFriday || nextThirdFriday || now).toISOString().split('T')[0];
  }
}

// Find nearest valid expiration: tries target, then -1 day, then +1 day, then -2 day, then +2 day
function findValidExpiration(targetDate: string): string {
  const target = new Date(targetDate);
  const candidates = [
    target,
    new Date(target.getTime() - 1 * 24 * 60 * 60 * 1000), // -1 day
    new Date(target.getTime() + 1 * 24 * 60 * 60 * 1000), // +1 day
    new Date(target.getTime() - 2 * 24 * 60 * 60 * 1000), // -2 days
    new Date(target.getTime() + 2 * 24 * 60 * 60 * 1000), // +2 days
  ];
  for (const d of candidates) {
    const day = d.getDay();
    if (day !== 0 && day !== 6) return d.toISOString().split('T')[0]; // Skip weekends
  }
  return targetDate; // Fallback to original
}

function pickStrike(spotPrice: number, otmStrikes: number, optionType: 'call' | 'put', strikeInterval = 5): number {
  // Round spot to nearest strike interval
  const atm = Math.round(spotPrice / strikeInterval) * strikeInterval;
  if (optionType === 'call') return atm + otmStrikes * strikeInterval;
  return atm - otmStrikes * strikeInterval;
}

// Smart strike selection: target ~0.30 delta for best risk/reward
function pickSmartStrike(
  spotPrice: number, optionType: 'call' | 'put', T: number, sigma: number,
  strikeInterval: number, budget: number, targetDelta = 0.30
): { strike: number; premium: number; delta: number } {
  const atm = Math.round(spotPrice / strikeInterval) * strikeInterval;
  const R = 0.05;
  
  // Scan strikes from 10 ITM to 10 OTM
  let bestStrike = atm;
  let bestPremium = blackScholes(spotPrice, atm, T, R, sigma, optionType);
  let bestDelta = 0.5; // ATM delta is ~0.5
  let bestDeltaDiff = Math.abs(0.5 - targetDelta);
  
  for (let offset = -10; offset <= 10; offset++) {
    const s = atm + offset * strikeInterval;
    if (s <= 0) continue;
    
    const p = blackScholes(spotPrice, s, T, R, sigma, optionType);
    if (p <= 0.01) continue;
    
    // Approximate delta using Black-Scholes
    const d1 = (Math.log(spotPrice / s) + (R + 0.5 * sigma * sigma) * T) / (sigma * Math.sqrt(T));
    let delta: number;
    if (optionType === 'call') {
      delta = normCDF(d1);
    } else {
      delta = Math.abs(normCDF(d1) - 1); // Put delta as positive number
    }
    
    const deltaDiff = Math.abs(delta - targetDelta);
    const affordable = p * 100 <= budget;
    
    // Pick strike closest to target delta that's within budget
    if (affordable && deltaDiff < bestDeltaDiff) {
      bestStrike = s;
      bestPremium = p;
      bestDelta = delta;
      bestDeltaDiff = deltaDiff;
    }
  }
  
  return { strike: bestStrike, premium: bestPremium, delta: bestDelta };
}

// ─────────────────────────────────────────────
// SETTINGS INTERFACE
// ─────────────────────────────────────────────

interface BotSettings {
  atrLength: number; atrMultiplier: number; emaLength: number;
  adxLength: number; adxThreshold: number; symbol: string;
  dollarAmount: number; interval: string; tradeDirection: string;
  expiryType: string; otmStrikes: number;
  strikeMode: string; manualStrike: number | null;
  takeProfitPct: number; stopLossPct: number;
  marketOpenDelayMin: number;
  botSignal: string;
}

// ─────────────────────────────────────────────
// ALPACA OPTIONS TRADING
// ─────────────────────────────────────────────

// Format option symbol for Alpaca: SPY240531C00580000
function formatOptionSymbol(symbol: string, expirationDate: string, optionType: 'call' | 'put', strike: number): string {
  const date = new Date(expirationDate);
  const year = date.getFullYear().toString().slice(2); // 24
  const month = (date.getMonth() + 1).toString().padStart(2, '0'); // 06
  const day = date.getDate().toString().padStart(2, '0'); // 15
  const type = optionType === 'call' ? 'C' : 'P';
  const strikeStr = Math.round(strike * 1000).toString().padStart(8, '0'); // 00580000
  return `${symbol.toUpperCase()}${year}${month}${day}${type}${strikeStr}`;
}

// Place options order via Tastytrade
async function placeTastytradeOptionOrder(
  supabase: any,
  userId: string,
  symbol: string,
  expirationDate: string,
  optionType: 'call' | 'put',
  strike: number,
  side: 'Buy to Open' | 'Sell to Close',
  qty: number
): Promise<{ success: boolean; orderId?: string; error?: string; status?: string; fillPrice?: number }> {
  try {
    const { data: creds } = await supabase.from('broker_credentials')
      .select('credentials')
      .eq('user_id', userId)
      .eq('broker', 'tastytrade')
      .maybeSingle();

    if (!creds?.credentials?.refresh_token) {
      return { success: false, error: 'No Tastytrade credentials found' };
    }

    // Get fresh access token
    const tokenRes = await fetch(`${Deno.env.get('SUPABASE_URL')}/functions/v1/tasty-oauth?action=refresh&user_id=${userId}`, {
      headers: { 'Authorization': `Bearer ${Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')}` }
    });
    const tokenJson = await tokenRes.json();
    if (!tokenJson.access_token) return { success: false, error: 'Failed to get access token' };

    const accessToken = tokenJson.access_token;
    const accountNumber = creds.credentials.account_number;
    if (!accountNumber) return { success: false, error: 'No account number found' };

    // Format Tastytrade OCC option symbol: SPY 260523C00590000
    const expParts = expirationDate.split('-');
    const yy = expParts[0].slice(2);
    const mm = expParts[1];
    const dd = expParts[2];
    const typeChar = optionType === 'call' ? 'C' : 'P';
    const strikeStr = String(Math.round(strike * 1000)).padStart(8, '0');
    const occSymbol = `${symbol}  ${yy}${mm}${dd}${typeChar}${strikeStr}`;

    const orderBody = {
      'order-type': 'Market',
      'time-in-force': 'Day',
      legs: [{
        'instrument-type': 'Equity Option',
        symbol: occSymbol,
        quantity: qty,
        action: side,
      }]
    };

    console.log(`[TastyOptions] Placing order: ${side} ${qty}x ${occSymbol} on account ${accountNumber}`);

    const res = await fetch(`https://api.tastytrade.com/accounts/${accountNumber}/orders`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${accessToken}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(orderBody),
    });

    const orderJson = await res.json();

    if (!res.ok) {
      const errMsg = orderJson?.error?.message || orderJson?.errors?.[0]?.message || JSON.stringify(orderJson);
      console.error('[TastyOptions] Order failed:', errMsg);
      return { success: false, error: errMsg, status: 'failed' };
    }

    const order = orderJson?.data?.order;
    const orderId = order?.id ? String(order.id) : null;
    const orderStatus = order?.status || 'received';
    console.log(`[TastyOptions] Order placed: id=${orderId} status=${orderStatus}`);

    // Poll up to 10s for fill price
    let fillPrice: number | undefined;
    if (orderId) {
      for (let i = 0; i < 5; i++) {
        await new Promise(r => setTimeout(r, 2000));
        const pollRes = await fetch(`https://api.tastytrade.com/accounts/${accountNumber}/orders/${orderId}`, {
          headers: { 'Authorization': `Bearer ${accessToken}` }
        });
        const polled = await pollRes.json();
        const filledOrder = polled?.data;
        const legs = filledOrder?.legs || [];
        const avgFill = legs[0]?.['average-fill-price'] || filledOrder?.['average-fill-price'];
        console.log(`[TastyOptions] Poll ${i+1}: status=${filledOrder?.status} avg_fill=$${avgFill}`);
        if (avgFill) { fillPrice = Number(avgFill); break; }
        if (filledOrder?.status === 'Filled') { fillPrice = Number(avgFill); break; }
      }
    }

    return { success: true, orderId: orderId || undefined, status: orderStatus, fillPrice };
  } catch (err) {
    console.error('[TastyOptions] Error:', err);
    return { success: false, error: String(err), status: 'error' };
  }
}

// Place options order via Alpaca
async function placeAlpacaOptionOrder(
  supabase: any,
  userId: string,
  symbol: string,
  expirationDate: string,
  optionType: 'call' | 'put',
  strike: number,
  side: 'buy' | 'sell',
  qty: number
): Promise<{ success: boolean; orderId?: string; error?: string; status?: string; fillPrice?: number }> {
  try {
    // Fetch Alpaca credentials
    const { data: creds } = await supabase
      .from('broker_credentials')
      .select('credentials')
      .eq('user_id', userId)
      .eq('broker', 'alpaca')
      .maybeSingle();

    if (!creds) {
      return { success: false, error: 'No Alpaca credentials found' };
    }

    const { api_key, secret_key, env } = creds.credentials;
    const baseUrl = env === 'live'
      ? 'https://api.alpaca.markets'
      : 'https://paper-api.alpaca.markets';

    const optionSymbol = formatOptionSymbol(symbol, expirationDate, optionType, strike);

    const orderBody = {
      symbol: optionSymbol,
      side,
      type: 'market',
      time_in_force: 'day',
      qty: String(qty),
    };

    console.log(`[AlpacaOptions] Placing order: ${side} ${qty} x ${optionSymbol}`);

    const res = await fetch(`${baseUrl}/v2/orders`, {
      method: 'POST',
      headers: {
        'APCA-API-KEY-ID': api_key,
        'APCA-API-SECRET-KEY': secret_key,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(orderBody),
    });

    const order = await res.json();

    if (!res.ok) {
      console.error('[AlpacaOptions] Order failed:', order.message || order);
      return { success: false, error: order.message || 'Alpaca order failed', status: 'failed' };
    }

    console.log(`[AlpacaOptions] Order placed: ${order.id} status=${order.status} filled_avg_price=${order.filled_avg_price}`);

    // If not immediately filled, poll up to 10s for fill price
    let fillPrice: number | undefined = order.filled_avg_price ? Number(order.filled_avg_price) : undefined;
    if (!fillPrice && order.status !== 'filled') {
      for (let i = 0; i < 5; i++) {
        await new Promise(r => setTimeout(r, 2000));
        const pollRes = await fetch(`${baseUrl}/v2/orders/${order.id}`, {
          headers: { 'APCA-API-KEY-ID': api_key, 'APCA-API-SECRET-KEY': secret_key }
        });
        const polled = await pollRes.json();
        console.log(`[AlpacaOptions] Poll ${i+1}: status=${polled.status} filled_avg_price=${polled.filled_avg_price}`);
        if (polled.filled_avg_price) { fillPrice = Number(polled.filled_avg_price); break; }
        if (polled.status === 'filled') { fillPrice = Number(polled.filled_avg_price); break; }
      }
    }

    return { success: true, orderId: order.id, status: order.status, fillPrice };

  } catch (err) {
    console.error('[AlpacaOptions] Error:', err);
    return { success: false, error: String(err), status: 'error' };
  }
}

// ─────────────────────────────────────────────
// MAIN HANDLER
// ─────────────────────────────────────────────

Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') return new Response('ok', { headers: corsHeaders });

  const supabase = createClient(
    Deno.env.get('SUPABASE_URL')!,
    Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')!
  );

  // GET /portfolio-value?bot_id=xxx — returns cash + live value of open positions
  if (req.method === 'GET') {
    const url = new URL(req.url);
    const botId = url.searchParams.get('bot_id');
    if (!botId) return new Response(JSON.stringify({ error: 'bot_id required' }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

    const { data: bot } = await supabase.from('options_bots').select('paper_balance, bot_interval').eq('id', botId).single();
    const cash = Number(bot?.paper_balance ?? 100000);
    const interval = bot?.bot_interval ?? '1h';

    const { data: openTrades } = await supabase.from('options_trades').select('*').eq('bot_id', botId).eq('status', 'open');
    let openValue = 0;
    const R = 0.05;
    if (openTrades && openTrades.length > 0) {
      for (const t of openTrades) {
        try {
          const candles = await fetchCandles(t.symbol, interval, 60);
          if (!candles.length) { openValue += Number(t.total_cost); continue; }
          const price = candles[candles.length - 1].close;
          const sigma = calcHistoricalVolatility(candles.map(c => c.close), 20, interval);
          const expDate = new Date(t.expiration_date);
          const T = Math.max(0, (expDate.getTime() - Date.now()) / (365 * 24 * 60 * 60 * 1000));
          const currentPremium = blackScholes(price, t.strike, T, R, sigma, t.option_type);
          openValue += currentPremium * t.contracts * 100;
        } catch (_) { openValue += Number(t.total_cost); }
      }
    }

    return new Response(JSON.stringify({ cash, open_value: openValue, total: cash + openValue }), {
      status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }

  // Parse body once for all POST handlers
  let _parsedBody: any = null;
  if (req.method === 'POST') {
    _parsedBody = await req.json().catch(() => ({}));
  }

  // ── INSTANT ACTIONS (POST with action field) ──
  if (req.method === 'POST') {
    const body = _parsedBody;
    const action = body.action;

    // Fetch current option price for frontend P&L display
    if (action === 'get_option_price') {
      const { symbol, strike, expiration, option_type } = body;
      if (!symbol || !strike || !expiration || !option_type) return new Response(JSON.stringify({ price: null }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      const price = await fetchRealOptionPrice(symbol, Number(strike), expiration, option_type);
      return new Response(JSON.stringify({ price }), { headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // Instant TP/SL check: called immediately when user saves new thresholds
    if (action === 'check_tpsl') {
      const botId = body.bot_id;
      if (!botId) return new Response(JSON.stringify({ error: 'bot_id required' }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

      const { data: bot } = await supabase.from('options_bots').select('*').eq('id', botId).single();
      if (!bot) return new Response(JSON.stringify({ error: 'Bot not found' }), { status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

      const takeProfitPct = Number(body.take_profit_pct ?? bot.take_profit_pct ?? 100);
      const stopLossPct   = Number(body.stop_loss_pct  ?? bot.stop_loss_pct  ?? 20);
      const interval      = bot.bot_interval ?? '1h';
      const R = 0.05;
      const closed: object[] = [];

      const { data: openTrades } = await supabase.from('options_trades').select('*').eq('bot_id', botId).eq('status', 'open');
      for (const open of (openTrades || [])) {
        try {
          let optionPrice = await fetchRealOptionPrice(open.symbol, open.strike, open.expiration_date, open.option_type, interval, bot.user_id);
          if (!optionPrice || optionPrice <= 0) {
            const candles = await fetchCandles(open.symbol, interval, 60);
            if (!candles.length) continue;
            const spotPrice = candles[candles.length - 1].close;
            const sigma = calcHistoricalVolatility(candles.map((c: any) => c.close), 20, interval);
            const expDate = new Date(open.expiration_date);
            const T = Math.max(0, (expDate.getTime() - Date.now()) / (365 * 24 * 60 * 60 * 1000));
            optionPrice = blackScholes(spotPrice, open.strike, T, R, sigma, open.option_type);
          }
          const pctChange = ((optionPrice - open.premium_per_contract) / open.premium_per_contract) * 100;
          const slThreshold = stopLossPct < 0 ? stopLossPct : -Math.abs(stopLossPct);
          if (pctChange >= takeProfitPct || pctChange <= slThreshold) {
            const pnl = (optionPrice - open.premium_per_contract) * open.contracts * 100;
            await supabase.from('options_trades').update({ status: 'closed', exit_price: optionPrice, pnl, closed_at: new Date().toISOString() }).eq('id', open.id);
            if (bot.broker === 'paper') {
              const { data: bRow } = await supabase.from('options_bots').select('paper_balance').eq('id', botId).single();
              const bal = Number(bRow?.paper_balance ?? 100000);
              await supabase.from('options_bots').update({ paper_balance: bal + Number(open.total_cost) + pnl }).eq('id', botId);
            }
            closed.push({ id: open.id, symbol: open.symbol, pct_change: pctChange.toFixed(1) + '%', pnl: pnl.toFixed(2), reason: pctChange >= takeProfitPct ? 'take_profit' : 'stop_loss' });
          }
        } catch (_) {}
      }
      return new Response(JSON.stringify({ checked: (openTrades || []).length, closed }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // Fast TP/SL Daemon: checks ALL open positions every 30 seconds for instant exit
    if (action === 'tpsl_daemon') {
      const now = new Date();
      
      // Get all open trades with their bot settings
      const { data: openTrades } = await supabase.from('options_trades')
        .select('*, options_bots!inner(take_profit_pct, stop_loss_pct, bot_interval, broker, user_id, name)')
        .eq('status', 'open');
      
      if (!openTrades || openTrades.length === 0) {
        return new Response(JSON.stringify({ checked: 0, closed: [], message: 'No open positions' }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
      }
      
      console.log(`[TPSL_Daemon] Checking ${openTrades.length} open positions for TP/SL...`);
      const closed: object[] = [];
      const R = 0.05;
      
      for (const open of openTrades) {
        try {
          const bot = (open as any).options_bots;
          const takeProfitPct = Number(bot?.take_profit_pct ?? 35);
          const stopLossPct = Number(bot?.stop_loss_pct ?? -25);
          const interval = bot?.bot_interval ?? '1h';
          const userId = bot?.user_id;
          
          // Fetch real-time price
          let optionPrice = await fetchRealOptionPrice(open.symbol, open.strike, open.expiration_date, open.option_type, interval, userId);
          const source = optionPrice > 0 ? 'tastytrade/polygon' : 'fallback-bs';
          
          if (!optionPrice || optionPrice <= 0) {
            // Fallback to Black-Scholes
            const candles = await fetchCandles(open.symbol, interval, 60);
            if (!candles.length) continue;
            const spotPrice = candles[candles.length - 1].close;
            const sigma = calcHistoricalVolatility(candles.map((c: any) => c.close), 20, interval);
            const expDate = new Date(open.expiration_date);
            const T = Math.max(0, (expDate.getTime() - Date.now()) / (365 * 24 * 60 * 60 * 1000));
            optionPrice = blackScholes(spotPrice, open.strike, T, R, sigma, open.option_type);
          }
          
          const pctChange = ((optionPrice - open.premium_per_contract) / open.premium_per_contract) * 100;
          const slThreshold = stopLossPct < 0 ? stopLossPct : -Math.abs(stopLossPct);
          const shouldTP = pctChange >= takeProfitPct;
          const shouldSL = pctChange <= slThreshold;
          
          console.log(`[TPSL_Daemon] ${open.symbol} ${open.option_type} $${open.strike}: current=$${optionPrice.toFixed(2)} entry=$${Number(open.premium_per_contract).toFixed(2)} pct=${pctChange.toFixed(1)}% tp=${takeProfitPct}% sl=${slThreshold}% shouldTP=${shouldTP} shouldSL=${shouldSL} source=${source}`);
          
          if (shouldTP || shouldSL) {
            const pnl = (optionPrice - open.premium_per_contract) * open.contracts * 100;
            await supabase.from('options_trades').update({ 
              status: 'closed', 
              exit_price: optionPrice, 
              pnl, 
              closed_at: now.toISOString(),
              exit_reason: shouldTP ? 'take_profit' : 'stop_loss'
            }).eq('id', open.id);
            
            // Update paper balance if paper trading
            if (bot?.broker === 'paper') {
              const { data: bRow } = await supabase.from('options_bots').select('paper_balance').eq('id', open.bot_id).single();
              const bal = Number(bRow?.paper_balance ?? 100000);
              await supabase.from('options_bots').update({ paper_balance: bal + Number(open.total_cost) + pnl }).eq('id', open.bot_id);
            }
            
            closed.push({ 
              id: open.id, 
              bot_name: bot?.name || 'Unknown',
              symbol: open.symbol, 
              strike: open.strike,
              pct_change: pctChange.toFixed(1) + '%', 
              pnl: pnl.toFixed(2), 
              reason: shouldTP ? 'take_profit' : 'stop_loss',
              source
            });
          }
        } catch (err) {
          console.log(`[TPSL_Daemon] Error checking trade ${open.id}:`, err);
        }
      }
      
      console.log(`[TPSL_Daemon] Closed ${closed.length} positions`);
      return new Response(JSON.stringify({ checked: openTrades.length, closed }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

    // Instant manual close: called when user clicks "Close Now" on a specific trade
    if (action === 'close_trade') {
      const tradeId = body.trade_id;
      if (!tradeId) return new Response(JSON.stringify({ error: 'trade_id required' }), { status: 400, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

      const { data: open } = await supabase.from('options_trades').select('*').eq('id', tradeId).single();
      if (!open) return new Response(JSON.stringify({ error: 'Trade not found' }), { status: 404, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });

      const { data: bot } = await supabase.from('options_bots').select('*').eq('id', open.bot_id).single();
      const interval = bot?.bot_interval ?? '1h';
      const R = 0.05;

      let optionPrice = await fetchRealOptionPrice(open.symbol, open.strike, open.expiration_date, open.option_type, interval, bot?.user_id);
      if (!optionPrice || optionPrice <= 0) {
        const candles = await fetchCandles(open.symbol, interval, 60);
        if (candles.length) {
          const spotPrice = candles[candles.length - 1].close;
          const sigma = calcHistoricalVolatility(candles.map((c: any) => c.close), 20, interval);
          const expDate = new Date(open.expiration_date);
          const T = Math.max(0, (expDate.getTime() - Date.now()) / (365 * 24 * 60 * 60 * 1000));
          optionPrice = blackScholes(spotPrice, open.strike, T, R, sigma, open.option_type);
        }
      }
      if (!optionPrice || optionPrice <= 0) optionPrice = Number(open.premium_per_contract);

      const pnl = (optionPrice - open.premium_per_contract) * open.contracts * 100;
      await supabase.from('options_trades').update({ status: 'closed', exit_price: optionPrice, pnl, closed_at: new Date().toISOString() }).eq('id', tradeId);

      if (bot && bot.broker === 'paper') {
        const bal = Number(bot.paper_balance ?? 100000);
        await supabase.from('options_bots').update({ paper_balance: bal + Number(open.total_cost) + pnl }).eq('id', open.bot_id);
      }
      return new Response(JSON.stringify({ success: true, symbol: open.symbol, exit_price: optionPrice, pnl: pnl.toFixed(2) }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    }

  }

  try {
    let targetBotId: string | null = null;
    let targetUserId: string | null = null;
    let forceRun = false;

    // INDEPENDENT MODE: Options bot runs on its own schedule via cron
    // No sync trigger from stock bot - generates its own signals
    if (req.method === 'POST') {
      const authHeader = req.headers.get('Authorization');
      const body = _parsedBody || {};
      const cronSecret = body.cron_secret;
      const validCron  = cronSecret === Deno.env.get('CRON_SECRET');
      if (!validCron && authHeader) {
        const token = authHeader.replace('Bearer ', '');
        const { data: { user } } = await supabase.auth.getUser(token);
        if (user) targetUserId = user.id;
      }
      targetBotId = body.bot_id || null;
      targetUserId = targetUserId || body.user_id || null;
      forceRun = body.force === true;
    }

    let query = supabase.from('options_bots').select('*');
    if (!forceRun) query = query.eq('enabled', true).eq('auto_submit', true);
    if (targetBotId)  query = query.eq('id', targetBotId);
    if (targetUserId) query = query.eq('user_id', targetUserId);

    console.log(`[OptionsBot] Query: targetBotId=${targetBotId}, targetUserId=${targetUserId}, independent_mode=true`);

    const { data: bots, error: botErr } = await query;
    
    if (botErr) {
      console.error('[OptionsBot] Query error:', botErr);
    }
    console.log(`[OptionsBot] Found ${bots?.length || 0} bots`);
    if (bots && bots.length > 0) {
      console.log('[OptionsBot] Bot names:', bots.map(b => b.name).join(', '));
    }
    if (botErr) throw botErr;
    if (!bots || bots.length === 0) {
      return new Response(JSON.stringify({ message: 'No active options bots', debug: { targetBotId, targetUserId } }), {
        status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
      });
    }

    console.log(`Found ${bots.length} active bots:`, bots.map(b => ({ id: b.id, name: b.name, user_id: b.user_id?.slice(0,8), symbol: b.bot_symbol, scan_mode: b.bot_scan_mode })));

    const results: object[] = [];
    const R = 0.05; // risk-free rate
    const now = new Date();
    
    // Check market hours (options on stocks only trade 9:30 AM - 4:00 PM ET)
    // Use UTC offset for ET (UTC-5 or UTC-4 depending on DST)
    const utcHour = now.getUTCHours();
    const utcMinute = now.getUTCMinutes();
    const utcDay = now.getUTCDay();
    
    // Convert UTC to ET using proper timezone (handles DST automatically)
    const etNowStr = now.toLocaleString('en-US', { timeZone: 'America/New_York' });
    const etDate = new Date(etNowStr);
    let etHour = etDate.getHours();
    let etMinute = etDate.getMinutes();
    let etDay = etDate.getDay();
    
    const isWeekday = etDay >= 1 && etDay <= 5;
    const isOptionsMarketHours = isWeekday && (etHour > 9 || (etHour === 9 && etMinute >= 30)) && etHour < 16;
    
    // Wait 3 minutes after market open to avoid opening volatility
    const isAfter930Buffer = etHour > 9 || (etHour === 9 && etMinute >= 33);
    
    console.log(`[OptionsBot] Market hours check: ET=${etHour}:${etMinute}, day=${etDay}, weekday=${isWeekday}, open=${isOptionsMarketHours}, after930buffer=${isAfter930Buffer}`);

    const SCAN_STOCKS = [
      'AAPL','MSFT','AMZN','NVDA','TSLA','GOOG','GOOGL','META','NFLX','BRK-B',
      'JPM','BAC','WFC','V','MA','PG','KO','PFE','UNH','HD',
      'INTC','CSCO','ADBE','CRM','ORCL','AMD','QCOM','TXN','IBM','AVGO',
      'XOM','CVX','BA','CAT','MMM','GE','HON','LMT','NOC','DE',
      'C','GS','MS','AXP','BLK','SCHW','BK','SPGI','ICE',
      'MRK','ABBV','AMGN','BMY','LLY','GILD','JNJ','REGN','VRTX','BIIB',
      'WMT','COST','TGT','LOW','MCD','SBUX','NKE','BKNG',
      'SNAP','UBER','LYFT','SPOT','ZM','DOCU','PINS','ROKU','SHOP',
      'CVS','TMO','MDT','ISRG','F','GM',
      // High volatility growth stocks (great for options)
      'SNOW','CRWD','NET','DDOG','MDB','OKTA','SPLK','FSLR','ENPH','SEDG',
      'DKNG','CHPT','LCID','RIVN','HOOD','SOFI','AI','PLTR','ASML','MU',
      'LRCX','KLAC','AMAT','MRVL','NXPI','CDNS','SNPS','ANET','FTNT','PANW',
      'GME','AMC','BBBY','EXPR','KOSS','NAKD','SNDL','TLRY','ACB','CGC',
      // ETFs (high volume options)
      'QQQ','SPY','VOO','IVV','VTI','VUG','QQQM','SCHG','XLK','VGT','SMH','TQQQ',
    ];

    const SCAN_ETFS = [
      'QQQ','SPY','VOO','IVV','VTI','VUG','QQQM','SCHG','XLK','VGT','SMH','TQQQ',
    ];

    const SCAN_TOP10 = [
      'SMCI','TSLA','NVDA','COIN','PLTR','AMD','MRNA','MSTY','ENPH','VKTX','CCL',
    ];

    const SCAN_BOOF = [
      'QQQ','SPY','TSLA','NVDA',
    ];

    const SCAN_DUO = [
      'SPY','QQQ',
    ];

    const SCAN_TOP50 = [
      'SNGX','HTCO','ERAS','BIYA','ACST','ACB','AIXI','AMST','EOSE','JBLU',
      'LAES','SLS','BE','CIFR','RDW','IREN','BRLS','EDSA','KNSA','OMCL',
      'CVLT','CNC','HRI','NVTS','CLS','RBLX','PLTR','TSLA','NVDA','AMD',
      'META','NFLX','AMZN','SMCI','NVR','AZO','MELI','GEV','MPWR','CAR',
      'SPY','QQQ','AAPL','MSFT','GOOGL','AVGO','INTC','PYPL','SNAP','UBER',
    ];
    for (const bot of bots) {
      // INDEPENDENT MODE: Options bot runs on its own schedule, scans symbols like stock bot
      console.log(`[OptionsBot] Running bot "${bot.name}" independently`);
      
      // Check if bot should run based on run_interval_min
      const runIntervalMin = (bot.run_interval_min as number) ?? 15;
      const lastRunAt = bot.last_run_at ? new Date(bot.last_run_at as string) : null;
      const minutesSinceLastRun = lastRunAt ? (now.getTime() - lastRunAt.getTime()) / (1000 * 60) : Infinity;
      
      if (!forceRun && minutesSinceLastRun < runIntervalMin) {
        console.log(`[OptionsBot] Skipping "${bot.name}" - ran ${minutesSinceLastRun.toFixed(1)}m ago, interval=${runIntervalMin}m`);
        continue;
      }
      
      // Options only trade during market hours + delay buffer after 9:30 open
      const delayMin = (bot.market_open_delay_min as number) ?? 0;
      const isAfterOpenBuffer = etHour > 9 || (etHour === 9 && etMinute >= (30 + delayMin));
      if (!forceRun && (!isOptionsMarketHours || !isAfterOpenBuffer)) {
        console.log(`[OptionsBot] Skipping "${bot.name}" - markets closed or before ${30 + delayMin} min after open (ET=${etHour}:${etMinute}, delay=${delayMin}min)`);
        continue;
      }
      
      // 0DTE cutoff: Don't trade 0DTE after 2:00 PM ET (12:00 PM MT)
      // 0DTE options stop trading 2 hours before market close (4:00 PM ET)
      const expiryType = bot.bot_expiry_type ?? 'weekly';
      const isAfter2PM_ET = etHour >= 14; // 2:00 PM ET or later
      if (!forceRun && expiryType === '0dte' && isAfter2PM_ET) {
        console.log(`[OptionsBot] Skipping "${bot.name}" - 0DTE cutoff reached (after 2:00 PM ET, 2 hours before close)`);
        continue;
      }
      
      console.log(`[OptionsBot] Running "${bot.name}" | interval=${runIntervalMin}m | expiry=${expiryType}`);
      const settings: BotSettings = {
        atrLength:      bot.bot_atr_length     ?? 10,
        atrMultiplier:  bot.bot_atr_multiplier ?? 3.0,
        emaLength:      bot.bot_ema_length     ?? 50,
        adxLength:      bot.bot_adx_length     ?? 14,
        adxThreshold:   bot.bot_adx_threshold  ?? 10,
        symbol:         bot.bot_symbol         ?? 'SPY',
        dollarAmount:   bot.bot_dollar_amount  ?? 500,
        interval:       bot.bot_interval       ?? '1h',
        tradeDirection: bot.bot_trade_direction ?? 'both',
        expiryType:     bot.bot_expiry_type    ?? 'weekly',
        otmStrikes:     bot.bot_otm_strikes    ?? 1,
        strikeMode:     bot.bot_strike_mode    ?? 'budget',
        manualStrike:   bot.bot_manual_strike  ?? null,
        takeProfitPct:  bot.take_profit_pct    ?? 40,
        stopLossPct:    bot.stop_loss_pct      ?? 20,
        marketOpenDelayMin: bot.market_open_delay_min ?? 0,
        botSignal:      (bot.bot_signal as string) || 'supertrend',
      };

      const scanMode: string = (bot.bot_scan_mode as string) || 'single';
      
      // INDEPENDENT MODE: Build symbol list from bot's scan mode
      const symbolList: string[] = scanMode === 'scan_stocks' ? SCAN_STOCKS
        : scanMode === 'scan_etfs' ? SCAN_ETFS
        : scanMode === 'scan_top10' ? SCAN_TOP10
        : scanMode === 'scan_top50' ? SCAN_TOP50
        : scanMode === 'scan_boof' ? SCAN_BOOF
        : scanMode === 'scan_duo' ? SCAN_DUO
        : [settings.symbol];

      console.log(`[OptionsBot] "${bot.name}" | scanMode=${scanMode} | symbols=${symbolList.length} | list=[${symbolList.slice(0,5).join(',')}...${symbolList.slice(-3).join(',')}]`);

      try {
        // ── TP/SL check on all open positions using REAL option prices ──
        const { data: allOpen } = await supabase.from('options_trades').select('*').eq('bot_id', bot.id).eq('status', 'open');
        if (allOpen && allOpen.length > 0) {
          for (const open of allOpen) {
            try {
              // Build Tradier option symbol format: SPY241231C00580000
              const expDate = new Date(open.expiration_date);
              const yy = String(expDate.getFullYear()).slice(-2);
              const mm = String(expDate.getMonth() + 1).padStart(2, '0');
              const dd = String(expDate.getDate()).padStart(2, '0');
              const strikeCents = Math.round(open.strike * 1000);
              const optSymbol = `${open.symbol}${yy}${mm}${dd}${open.option_type.toUpperCase().charAt(0)}${String(strikeCents).padStart(8, '0')}`;
              
              // Fetch REAL option price from Tastytrade (or fallback to Polygon)
              let optionPrice = await fetchRealOptionPrice(open.symbol, open.strike, open.expiration_date, open.option_type, settings.interval, bot.user_id);
              const source = optionPrice > 0 ? 'tastytrade/polygon' : 'fallback-bs';
              
              if (!optionPrice || optionPrice <= 0) {
                const candles = await fetchCandles(open.symbol, settings.interval, 60);
                if (!candles.length) continue;
                const currentPrice = candles[candles.length - 1].close;
                const etfs = ['SPY','QQQ','IWM','DIA','GLD','TLT','XLF','XLE','XLK','XLV','EEM','VXX'];
                const highVol = ['TSLA','NVDA','AMD','MSTR','COIN','PLTR','GME','AMC','RIVN','LCID'];
                const iv = etfs.includes(open.symbol) ? 0.15 : highVol.includes(open.symbol) ? 0.45 : 0.25;
                const expParts = open.expiration_date.split('-');
                const expDateFixed = new Date(Date.UTC(Number(expParts[0]), Number(expParts[1]) - 1, Number(expParts[2]), 20, 0, 0));
                const T = Math.max(1 / (365 * 24 * 60), (expDateFixed.getTime() - Date.now()) / (365 * 24 * 60 * 60 * 1000));
                optionPrice = blackScholes(currentPrice, open.strike, T, R, iv, open.option_type);
              }
              
              const pctChange = ((optionPrice - open.premium_per_contract) / open.premium_per_contract) * 100;
              const shouldTP = pctChange >= settings.takeProfitPct;
              const slThreshold = settings.stopLossPct < 0 ? settings.stopLossPct : -Math.abs(settings.stopLossPct);
              const shouldSL = pctChange <= slThreshold;
              console.log(`[OptionsBot] TP/SL ${open.symbol} ${open.option_type} $${open.strike}: current=$${optionPrice.toFixed(2)} entry=$${Number(open.premium_per_contract).toFixed(2)} pct=${pctChange.toFixed(1)}% tp=${settings.takeProfitPct}% sl=${slThreshold}% shouldTP=${shouldTP} shouldSL=${shouldSL} source=${source}`);
              
              // EOD exit: 0DTE options — force close all positions at 2:00 PM ET
              // Compute ET date correctly: ET = UTC - 4 hours (DST) or -5 hours (standard)
              // Since March 10 - Nov 3 is DST, during most trading hours we use -4
              const isDST = etDate.getHours() !== now.getUTCHours(); // Simple DST check
              const etOffsetMs = (isDST ? 4 : 5) * 60 * 60 * 1000;
              const etAdjustedDate = new Date(now.getTime() - etOffsetMs);
              const etDateStr = etAdjustedDate.toISOString().split('T')[0];
              const is0DTE = open.expiration_date === etDateStr;
              const shouldEOD = is0DTE && isAfter2PM_ET;
              console.log(`[OptionsBot] EOD Check ${open.symbol} ${open.option_type} $${open.strike}: exp=${open.expiration_date} etDate=${etDateStr} is0DTE=${is0DTE} isAfter2PM=${isAfter2PM_ET} shouldEOD=${shouldEOD}`);
              
              if (shouldTP || shouldSL || shouldEOD) {
                const pnl = (optionPrice - open.premium_per_contract) * open.contracts * 100;
                let closeStatus = 'closed';
                let closeOrderId = null;
                let closeError = null;

                // Close live position
                if (bot.broker === 'tastytrade') {
                  console.log(`[OptionsBot] Closing Tastytrade position: ${open.contracts} contracts of ${open.symbol} ${open.option_type}`);
                  const tastyResult = await placeTastytradeOptionOrder(
                    supabase, bot.user_id, open.symbol, open.expiration_date,
                    open.option_type, open.strike, 'Sell to Close', open.contracts
                  );
                  if (tastyResult.success) {
                    closeStatus = 'closed';
                    closeOrderId = tastyResult.orderId;
                    if (tastyResult.fillPrice && tastyResult.fillPrice > 0) {
                      optionPrice = tastyResult.fillPrice; // use real exit price for P&L
                      console.log(`[OptionsBot] Tastytrade real exit price: $${optionPrice.toFixed(2)}/contract`);
                    }
                  } else {
                    closeError = tastyResult.error;
                    console.error(`[OptionsBot] Tastytrade close failed: ${closeError}`);
                  }
                } else if (bot.broker === 'alpaca' && open.order_id) {
                  console.log(`[OptionsBot] Closing Alpaca position: ${open.contracts} contracts of ${open.symbol} ${open.option_type}`);
                  const alpacaResult = await placeAlpacaOptionOrder(
                    supabase, bot.user_id, open.symbol, open.expiration_date,
                    open.option_type, open.strike, 'sell', open.contracts
                  );
                  if (alpacaResult.success) {
                    closeStatus = alpacaResult.status === 'filled' ? 'closed' : 'closing';
                    closeOrderId = alpacaResult.orderId;
                    if (alpacaResult.fillPrice && alpacaResult.fillPrice > 0) {
                      optionPrice = alpacaResult.fillPrice;
                    }
                  } else {
                    closeError = alpacaResult.error;
                    console.error(`[OptionsBot] Alpaca close failed: ${closeError}`);
                  }
                } else {
                  // Paper trading: update virtual balance
                  const { data: botRow } = await supabase.from('options_bots').select('paper_balance').eq('id', bot.id).single();
                  const bal = Number(botRow?.paper_balance ?? 100000);
                  await supabase.from('options_bots').update({ paper_balance: bal + (open.total_cost + pnl) }).eq('id', bot.id);
                }

                await supabase.from('options_trades').update({ 
                  status: closeStatus, 
                  exit_price: optionPrice, 
                  pnl, 
                  close_order_id: closeOrderId,
                  broker_error: closeError,
                  closed_at: new Date().toISOString() 
                }).eq('id', open.id);
                
                const exitReason = shouldEOD ? 'eod_exit' : shouldTP ? 'take_profit' : 'stop_loss';
                results.push({ bot_id: bot.id, symbol: open.symbol, status: exitReason, pct_change: pctChange.toFixed(1) + '%', pnl: pnl.toFixed(2), order_id: closeOrderId, broker_error: closeError });
              }
            } catch (_) {}
          }
        }

        const tradedThisRun = new Set<string>();
        for (const sym of symbolList) {
          try {
              const candles = await fetchCandles(sym, settings.interval, 150);
              if (candles.length < 60) { results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'Not enough candle data' }); continue; }

              // INDEPENDENT MODE: Always generate our own signal based on bot_signal setting
              let signal: 'buy' | 'sell' | 'none';
              let price: number;
              let reason: string;
              
              const botSignal = settings.botSignal || 'supertrend';
              let sigResult: { signal: 'buy' | 'sell' | 'none', price: number, reason: string, trend?: number, ema?: number, adx?: number };
              if (botSignal === 'rsi_macd') {
                sigResult = generateSignalRSIMACD(candles, settings.tradeDirection);
              } else if (botSignal === 'boof20') {
                sigResult = generateSignalBoof20(candles, settings.tradeDirection, 0.001, -0.001);
              } else if (botSignal === 'boof30') {
                sigResult = generateSignalBoof30(candles, settings.tradeDirection);
              } else {
                sigResult = generateSignal(candles, settings);
              }
              signal = sigResult.signal;
              price = sigResult.price;
              reason = sigResult.reason;
              
              console.log(`[OptionsBot] "${bot.name}" | ${sym} | SIGNAL: ${signal} | price=$${price.toFixed(2)} | signal_type=${botSignal} | ${reason}`);
              
              if (signal === 'none') { results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'no_signal' }); continue; }
              if (signal === 'buy'  && settings.tradeDirection === 'short') { results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'Direction filter' }); continue; }
              if (signal === 'sell' && settings.tradeDirection === 'long')  { results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'Direction filter' }); continue; }

              // In-memory dedup: prevent parallel batch from trading same symbol twice in one run
              if (tradedThisRun.has(sym)) { results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'Already traded this symbol in this run' }); continue; }
              tradedThisRun.add(sym);

              // Race condition prevention: check for any trade within 1 minute for this bot+symbol
              const oneMinuteAgo = new Date(Date.now() - 60 * 1000).toISOString();
              const { data: recent1m } = await supabase.from('options_trades').select('id').eq('bot_id', bot.id).eq('symbol', sym).gte('created_at', oneMinuteAgo).limit(1);
              if (recent1m && recent1m.length > 0) { results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: `Duplicate trade within 1 minute (race condition)` }); continue; }

              // Block if already in open position on this symbol for this bot
              const { data: existingOpen } = await supabase.from('options_trades').select('id').eq('bot_id', bot.id).eq('symbol', sym).eq('status', 'open').limit(1);
              if (existingOpen && existingOpen.length > 0) { results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'Already in open position' }); continue; }

              // Distributed lock: check for any trade inserted in last 5 seconds (concurrent invocation guard)
              const fiveSecAgo = new Date(Date.now() - 5 * 1000).toISOString();
              const { data: veryRecent } = await supabase.from('options_trades').select('id').eq('bot_id', bot.id).eq('symbol', sym).gte('created_at', fiveSecAgo).limit(1);
              if (veryRecent && veryRecent.length > 0) { results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'Concurrent invocation guard (5s lock)' }); continue; }

              // 5-minute cooldown between entries on same symbol for this bot
              const fiveMinAgo = new Date(Date.now() - 5 * 60 * 1000).toISOString();
              const { data: recent5m } = await supabase.from('options_trades').select('id').eq('bot_id', bot.id).eq('symbol', sym).eq('signal', 'buy').gte('created_at', fiveMinAgo).limit(1);
              if (recent5m && recent5m.length > 0) { results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: '5-minute cooldown between entries' }); continue; }

              // Close open opposite positions and return balance
              const { data: openTrades } = await supabase.from('options_trades').select('*').eq('bot_id', bot.id).eq('symbol', sym).eq('status', 'open');
              const sigma = calcHistoricalVolatility(candles.map(c => c.close), 20, settings.interval);
              if (openTrades && openTrades.length > 0) {
                for (const open of openTrades) {
                  const optType: 'call' | 'put' = open.option_type;
                  const expDate = new Date(open.expiration_date);
                  const T = Math.max(0, (expDate.getTime() - Date.now()) / (365 * 24 * 60 * 60 * 1000));
                  const exitPremium = blackScholes(price, open.strike, T, R, sigma, optType);
                  const pnl = (exitPremium - open.premium_per_contract) * open.contracts * 100;
                  
                  // Close via Alpaca if live trading
                  if (bot.broker === 'alpaca' && open.order_id) {
                    const alpacaResult = await placeAlpacaOptionOrder(
                      supabase,
                      bot.user_id,
                      open.symbol,
                      open.expiration_date,
                      open.option_type,
                      open.strike,
                      'sell',
                      open.contracts
                    );
                    if (alpacaResult.success) {
                      await supabase.from('options_trades').update({ 
                        status: 'closed', 
                        exit_price: exitPremium, 
                        pnl, 
                        close_order_id: alpacaResult.orderId,
                        closed_at: new Date().toISOString() 
                      }).eq('id', open.id);
                    } else {
                      await supabase.from('options_trades').update({ 
                        status: 'closed', 
                        exit_price: exitPremium, 
                        pnl, 
                        broker_error: alpacaResult.error,
                        closed_at: new Date().toISOString() 
                      }).eq('id', open.id);
                    }
                  } else {
                    // Paper trading
                    await supabase.from('options_trades').update({ status: 'closed', exit_price: exitPremium, pnl, closed_at: new Date().toISOString() }).eq('id', open.id);
                    // Return original cost + profit/loss back to balance
                    const { data: bRow } = await supabase.from('options_bots').select('paper_balance').eq('id', bot.id).single();
                    const bBal = Number(bRow?.paper_balance ?? 100000);
                    await supabase.from('options_bots').update({ paper_balance: bBal + Number(open.total_cost) + pnl }).eq('id', bot.id);
                  }
                }
              }

              // Determine option type based on signal and bot setting
              let optionType: 'call' | 'put';
              const botOptionType = bot.bot_option_type || 'both';
              if (botOptionType === 'call') {
                optionType = 'call';
              } else if (botOptionType === 'put') {
                optionType = 'put';
              } else {
                // 'both' - follow signal
                optionType = signal === 'buy' ? 'call' : 'put';
              }
              const targetExpiration = getExpirationDate(settings.expiryType);
              const expirationDate = findValidExpiration(targetExpiration);
              const alpacaCreds = bot.broker === 'alpaca' ? await supabase.from('broker_credentials').select('credentials').eq('user_id', bot.user_id).eq('broker', 'alpaca').maybeSingle().then((r: any) => r.data?.credentials) : null;

              // Fetch Tastytrade access token for ALL bots if user has Tastytrade connected
              // This ensures paper trading uses the same real data as live trading
              let tastyAccessToken: string | null = null;
              try {
                const { data: tastyCreds } = await supabase.from('broker_credentials')
                  .select('credentials').eq('user_id', bot.user_id).eq('broker', 'tastytrade').maybeSingle();
                if (tastyCreds?.credentials?.refresh_token) {
                  const tokenRes = await fetch(`${Deno.env.get('SUPABASE_URL')}/functions/v1/tasty-oauth?action=refresh&user_id=${bot.user_id}`, {
                    headers: { 'Authorization': `Bearer ${Deno.env.get('SUPABASE_SERVICE_ROLE_KEY')}` }
                  });
                  const tokenJson = await tokenRes.json();
                  if (tokenJson.access_token) tastyAccessToken = tokenJson.access_token;
                }
              } catch (_) {}

              // Spot price priority: Tastytrade → Polygon → Alpaca → Yahoo
              // Each source is sanity-checked. If no sane price found, SKIP the trade.
              let spotPrice: number | null = null;

              // 1. Try Tastytrade real-time
              if (tastyAccessToken) {
                spotPrice = await fetchTastytradeSpotPrice(sym, tastyAccessToken);
              }

              // 2. Try Polygon (reliable, near real-time for equities)
              if (!spotPrice) {
                spotPrice = await fetchPolygonSpotPrice(sym);
                if (spotPrice && !sanityCheckSpot(sym, spotPrice)) spotPrice = null;
              }

              // 3. Try Alpaca if connected
              if (!spotPrice && alpacaCreds?.api_key) {
                spotPrice = await fetchAlpacaSpotPrice(sym, alpacaCreds.api_key, alpacaCreds.secret_key);
                if (spotPrice && !sanityCheckSpot(sym, spotPrice)) spotPrice = null;
              }

              // 4. Yahoo fallback — sanity check required
              if (!spotPrice) {
                try {
                  const url = `https://query1.finance.yahoo.com/v8/finance/chart/${sym}?interval=1m&range=1d`;
                  const res = await fetch(url, { headers: { 'User-Agent': 'Mozilla/5.0' } });
                  const json = await res.json();
                  const meta = json?.chart?.result?.[0]?.meta;
                  const p = meta?.regularMarketPrice ?? meta?.price;
                  if (p && p > 0 && sanityCheckSpot(sym, p)) spotPrice = p;
                } catch (_) {}
              }

              // HARD STOP: if we can't get a sane spot price, don't trade
              if (!spotPrice || spotPrice <= 0) {
                console.log(`[OptionsBot] BLOCKED: Cannot get reliable spot price for ${sym} — skipping trade`);
                results.push({ bot_id: bot.id, symbol: sym, status: 'skipped', reason: 'No reliable spot price' });
                continue;
              }
              console.log(`[OptionsBot] Spot price for ${sym}: $${spotPrice} (broker=${bot.broker})`);
              const strikeInterval = spotPrice > 500 ? 5 : spotPrice > 100 ? 5 : spotPrice > 50 ? 2.5 : 1;
              const dollarAmount = bot.bot_dollar_amount || 500;

              // Strike selection:
              // - 0DTE: start 2 strikes ITM (higher delta ~0.65-0.75, moves more with stock)
              // - Weekly/Monthly: start ATM (~0.50 delta)
              // Walk toward OTM until 1-contract cost fits budget.
              // Hard minimum: $1.00/contract. Never buy cheap far-OTM garbage.
              const atmStrike = Math.round(spotPrice / strikeInterval) * strikeInterval;
              const MIN_PREMIUM = 1.00; // $100/contract minimum
              const MAX_STRIKES_WALK = 5; // max 5 strikes OTM — prevents deep OTM garbage
              const MAX_STRIKE_PCT_FROM_SPOT = 0.10; // never buy more than 10% OTM
              // For 0DTE, start ITM (negative offset = ITM for calls, positive = ITM for puts)
              const startOffset = expiryType === '0dte' ? -2 : 0;
              const startStrike = optionType === 'call'
                ? atmStrike + startOffset * strikeInterval  // calls: go lower = ITM
                : atmStrike - startOffset * strikeInterval; // puts: go higher = ITM
              let strike = startStrike;
              let premium = 0;

              for (let offset = 0; offset <= MAX_STRIKES_WALK; offset++) {
                const candidateStrike = optionType === 'call'
                  ? startStrike + offset * strikeInterval
                  : startStrike - offset * strikeInterval;
                const candidatePremium = await fetchRealOptionPrice(sym, candidateStrike, expirationDate, optionType, settings.interval, bot.user_id);
                // If Tastytrade credentials exist (paper or live): require a real price — no Black-Scholes estimates
                if (!candidatePremium || candidatePremium <= 0) {
                  if (tastyAccessToken) {
                    console.log(`[OptionsBot] SKIP: No real-time price for ${sym} $${candidateStrike} ${optionType} — not trading on estimates (broker=${bot.broker})`);
                    break;
                  }
                  continue;
                }

                // Too cheap — stop walking further OTM, it only gets worse
                if (candidatePremium < MIN_PREMIUM) {
                  console.log(`[OptionsBot] $${candidateStrike} premium $${candidatePremium.toFixed(2)} fell below $${MIN_PREMIUM} min — stopping walk`);
                  break;
                }

                // Sanity check: strike must be within 10% of spot price
                const pctFromSpot = Math.abs(candidateStrike - spotPrice) / spotPrice;
                if (pctFromSpot > MAX_STRIKE_PCT_FROM_SPOT) {
                  console.log(`[OptionsBot] BLOCKED deep OTM: $${candidateStrike} is ${(pctFromSpot*100).toFixed(1)}% from spot $${spotPrice.toFixed(2)} — stopping walk`);
                  break;
                }

                // 1 contract fits budget — use it
                if (candidatePremium * 100 <= dollarAmount) {
                  strike = candidateStrike;
                  premium = candidatePremium;
                  break;
                }

                console.log(`[OptionsBot] $${candidateStrike} @ $${candidatePremium.toFixed(2)}/contract ($${(candidatePremium*100).toFixed(0)}) exceeds budget $${dollarAmount} — trying next strike`);
              }

              // HARD STOP — never trade below $1.00 premium under any circumstance
              if (!premium || premium < MIN_PREMIUM) {
                console.log(`[OptionsBot] BLOCKED: ${sym} premium $${premium?.toFixed(2) ?? '0'} < $${MIN_PREMIUM} — refusing to trade`);
                continue;
              }

              const contracts = Math.max(1, Math.floor(dollarAmount / (premium * 100)));
              const totalCost = contracts * premium * 100;
              console.log(`[OptionsBot] Selected: ${sym} ${optionType} $${strike} @ $${premium.toFixed(2)}/contract x${contracts} = $${totalCost.toFixed(2)} (budget=$${dollarAmount} spot=$${spotPrice.toFixed(2)})`);


              let tradeStatus = 'open';
              let orderId = null;
              let brokerError = null;

              // Live trading
              if (bot.broker === 'tastytrade') {
                console.log(`[OptionsBot] Placing Tastytrade order: ${contracts} contracts of ${sym} ${optionType}`);
                const tastyResult = await placeTastytradeOptionOrder(
                  supabase, bot.user_id, sym, expirationDate, optionType, strike, 'Buy to Open', contracts
                );
                if (tastyResult.success) {
                  tradeStatus = 'open';
                  orderId = tastyResult.orderId;
                  if (tastyResult.fillPrice && tastyResult.fillPrice > 0) {
                    premium = tastyResult.fillPrice;
                    console.log(`[OptionsBot] Tastytrade real fill price: $${premium.toFixed(2)}/contract`);
                  } else {
                    console.log(`[OptionsBot] Tastytrade fill pending, using estimate: $${premium.toFixed(2)}/contract`);
                  }
                } else {
                  tradeStatus = 'failed';
                  brokerError = tastyResult.error;
                  console.error(`[OptionsBot] Tastytrade order failed: ${brokerError}`);
                }
              } else if (bot.broker === 'alpaca') {
                console.log(`[OptionsBot] Placing Alpaca order: ${contracts} contracts of ${sym} ${optionType}`);
                const alpacaResult = await placeAlpacaOptionOrder(
                  supabase, bot.user_id, sym, expirationDate, optionType, strike, 'buy', contracts
                );
                if (alpacaResult.success) {
                  tradeStatus = alpacaResult.status === 'filled' ? 'filled' : 'pending';
                  orderId = alpacaResult.orderId;
                  if (alpacaResult.fillPrice && alpacaResult.fillPrice > 0) {
                    premium = alpacaResult.fillPrice;
                    console.log(`[OptionsBot] Alpaca real fill price: $${premium.toFixed(2)}/contract`);
                  }
                } else {
                  tradeStatus = 'failed';
                  brokerError = alpacaResult.error;
                  console.error(`[OptionsBot] Alpaca order failed: ${brokerError}`);
                }
              } else {
                // Paper trading: update virtual balance
                const { data: botRow } = await supabase.from('options_bots').select('paper_balance').eq('id', bot.id).single();
                const currentBalance = Number(botRow?.paper_balance ?? 100000);
                await supabase.from('options_bots').update({ paper_balance: Math.max(0, currentBalance - totalCost) }).eq('id', bot.id);
              }

              console.log(`[OptionsBot] Inserting trade: ${sym} ${optionType} strike=${strike} premium=$${premium.toFixed(2)} contracts=${contracts} total=$${totalCost.toFixed(2)} status=${tradeStatus}`);
              const { error: insertErr } = await supabase.from('options_trades').insert({
                user_id: bot.user_id, bot_id: bot.id, symbol: sym,
                option_type: optionType, strike, expiration_date: expirationDate,
                contracts, premium_per_contract: premium, total_cost: totalCost,
                entry_price: premium, status: tradeStatus, signal, reason,
                broker: bot.broker || 'paper',
                broker_error: brokerError,
                created_at: new Date().toISOString(),
              });
              if (insertErr) console.error(`[OptionsBot] INSERT FAILED for ${sym}:`, insertErr.message);

              results.push({ bot_id: bot.id, status: tradeStatus, symbol: sym, option_type: optionType, strike, expiration_date: expirationDate, contracts, premium: premium.toFixed(2), total_cost: totalCost.toFixed(2), budget: dollarAmount, order_id: orderId, broker_error: brokerError, sigma: (sigma * 100).toFixed(1) + '%', signal, reason });

          } catch (err) {
            results.push({ bot_id: bot.id, symbol: sym, status: 'error', error: String(err) });
          }
        }
      } catch (err) {
        results.push({ bot_id: bot.id, status: 'error', error: String(err) });
      }
      
      // Update last_run_at after successful processing
      await supabase.from('options_bots').update({ last_run_at: now.toISOString() }).eq('id', bot.id);
    }

    console.log(`Processed ${results.length} results:`, results);

    return new Response(JSON.stringify({ processed: results.length, results }), {
      status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });

  } catch (err) {
    return new Response(JSON.stringify({ error: String(err) }), {
      status: 500, headers: { ...corsHeaders, 'Content-Type': 'application/json' }
    });
  }
});
