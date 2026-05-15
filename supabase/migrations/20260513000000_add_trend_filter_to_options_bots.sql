-- Add trend_filter column to options_bots table
ALTER TABLE options_bots ADD COLUMN IF NOT EXISTS trend_filter TEXT DEFAULT 'none';
