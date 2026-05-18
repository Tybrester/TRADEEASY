-- Add per-symbol TP/SL/direction override rules to stock_bots
ALTER TABLE stock_bots ADD COLUMN IF NOT EXISTS symbol_rules JSONB DEFAULT '[]'::jsonb;
