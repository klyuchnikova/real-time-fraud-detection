CREATE TABLE IF NOT EXISTS transaction_scores (
    received_order BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    transaction_id TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL CHECK (score >= 0 AND score <= 1),
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
    model_version TEXT NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
-- Explicit migration for a pre-existing scoring table.
ALTER TABLE transaction_scores ADD COLUMN IF NOT EXISTS model_version TEXT;
UPDATE transaction_scores SET model_version = 'legacy' WHERE model_version IS NULL;
ALTER TABLE transaction_scores ALTER COLUMN model_version SET NOT NULL;
CREATE INDEX IF NOT EXISTS transaction_scores_fraud_recent
    ON transaction_scores (received_order DESC) WHERE fraud_flag = 1;
