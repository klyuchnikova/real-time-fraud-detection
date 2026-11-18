-- Additive and repeatable. A score may arrive before its context.
ALTER TABLE transaction_scores ADD COLUMN IF NOT EXISTS scored_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS transaction_scores_scored_at ON transaction_scores(scored_at);
CREATE TABLE IF NOT EXISTS transaction_context (
    transaction_id TEXT PRIMARY KEY,
    us_state TEXT NOT NULL,
    merch TEXT NOT NULL,
    cat_id TEXT NOT NULL,
    amount NUMERIC NOT NULL CHECK (amount >= 0),
    emitted_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS transaction_context_state_merch ON transaction_context(us_state, merch);
CREATE OR REPLACE VIEW monitoring_scores AS
SELECT s.received_order, s.transaction_id, s.score, s.fraud_flag, s.model_version,
       s.received_at, s.scored_at, c.us_state, c.merch, c.cat_id, c.amount,
       c.emitted_at, c.transaction_id IS NOT NULL AS has_context
FROM transaction_scores s LEFT JOIN transaction_context c USING (transaction_id);

