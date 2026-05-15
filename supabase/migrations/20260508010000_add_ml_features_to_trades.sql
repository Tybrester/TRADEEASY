-- Add ML features to options_trades for Boof 4.0 training

-- Entry market state
alter table public.options_trades add column if not exists entry_regime text;
alter table public.options_trades add column if not exists entry_rsi numeric;
alter table public.options_trades add column if not exists entry_slope numeric;
alter table public.options_trades add column if not exists entry_atr numeric;
alter table public.options_trades add column if not exists entry_spot numeric;
alter table public.options_trades add column if not exists entry_ema numeric;

-- Time features
alter table public.options_trades add column if not exists hour_of_day integer;
alter table public.options_trades add column if not exists day_of_week integer;

-- Additional exit data
alter table public.options_trades add column if not exists exit_regime text;
alter table public.options_trades add column if not exists max_pnl_reached numeric;
alter table public.options_trades add column if not exists min_pnl_reached numeric;

-- Signal metadata
alter table public.options_trades add column if not exists signal_version text default 'boof30';
