alter table options_bots add column if not exists last_signal text default null;
alter table options_bots add column if not exists last_signal_at timestamptz default null;
