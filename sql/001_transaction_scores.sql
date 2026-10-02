CREATE TABLE IF NOT EXISTS transaction_scores (
    received_order BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    transaction_id TEXT PRIMARY KEY,
    score DOUBLE PRECISION NOT NULL CHECK (score >= 0 AND score <= 1),
    fraud_flag SMALLINT NOT NULL CHECK (fraud_flag IN (0, 1)),
    received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS transaction_scores_fraud_recent
    ON transaction_scores (received_order DESC) WHERE fraud_flag = 1;
