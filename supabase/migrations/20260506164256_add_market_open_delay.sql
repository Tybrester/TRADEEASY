-- Add market_open_delay_min column to options_bots table
alter table options_bots add column market_open_delay_min integer default 0;
