CREATE TABLE IF NOT EXISTS dataset_sources (
    sha256 TEXT PRIMARY KEY, rows_loaded BIGINT NOT NULL, loaded_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS labeled_transactions (
    transaction_id TEXT PRIMARY KEY,
    transaction_time TIMESTAMP NOT NULL,
    target SMALLINT NOT NULL CHECK (target IN (0,1)),
    data JSONB NOT NULL,
    batch_id TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS labeled_transactions_time ON labeled_transactions(transaction_time);
CREATE TABLE IF NOT EXISTS training_state (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK(singleton),
    next_test_month DATE NOT NULL,
    pending_version TEXT
);
CREATE TABLE IF NOT EXISTS training_runs (
    version TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    window_start DATE NOT NULL,
    test_month DATE NOT NULL,
    baseline_version TEXT,
    status TEXT NOT NULL DEFAULT 'reserved',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS supply_runs (
    batch_id TEXT PRIMARY KEY, run_id TEXT UNIQUE NOT NULL, target_month DATE NOT NULL UNIQUE,
    status TEXT NOT NULL DEFAULT 'reserved', source_ids JSONB NOT NULL,
    row_count INTEGER, created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS training_requests (
    run_id TEXT PRIMARY KEY, version TEXT NOT NULL REFERENCES training_runs(version)
);
CREATE TABLE IF NOT EXISTS supply_requests (
    run_id TEXT PRIMARY KEY, batch_id TEXT NOT NULL REFERENCES supply_runs(batch_id)
);
