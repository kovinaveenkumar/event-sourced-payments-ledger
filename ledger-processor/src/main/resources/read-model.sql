-- Read tables filled by the ledger processor. Any of them can be emptied and rebuilt by
-- replaying the ledger-events topic from the start (scripts/replay-projections.sh).

CREATE TABLE IF NOT EXISTS account_view (
    account_id        UUID         PRIMARY KEY,
    owner             VARCHAR(120) NOT NULL,
    currency          CHAR(3)      NOT NULL,
    balance_cents     BIGINT       NOT NULL,
    version           BIGINT       NOT NULL,
    transaction_count BIGINT       NOT NULL,
    opened_at         TIMESTAMPTZ  NOT NULL,
    updated_at        TIMESTAMPTZ  NOT NULL
);

CREATE TABLE IF NOT EXISTS account_transactions (
    event_id                UUID         PRIMARY KEY,
    account_id              UUID         NOT NULL,
    event_type              VARCHAR(32)  NOT NULL,
    direction               VARCHAR(3)   NOT NULL,
    amount_cents            BIGINT       NOT NULL,
    balance_after_cents     BIGINT       NOT NULL,
    transfer_id             UUID,
    counterparty_account_id UUID,
    occurred_at             TIMESTAMPTZ  NOT NULL
);
CREATE INDEX IF NOT EXISTS account_transactions_account_idx ON account_transactions (account_id, occurred_at DESC);

CREATE TABLE IF NOT EXISTS fraud_alerts (
    alert_id    UUID         PRIMARY KEY,
    account_id  UUID         NOT NULL,
    rule        VARCHAR(32)  NOT NULL,
    detail      VARCHAR(300) NOT NULL,
    event_id    UUID         NOT NULL,
    occurred_at TIMESTAMPTZ  NOT NULL,
    created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS fraud_alerts_created_idx ON fraud_alerts (created_at DESC);

-- Set once the alert has been sent to the fraud-alerts Kafka topic (outbox for alerts).
ALTER TABLE fraud_alerts ADD COLUMN IF NOT EXISTS published_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS fraud_alerts_unpublished_idx ON fraud_alerts (created_at) WHERE published_at IS NULL;

-- Messages that couldn't be read or applied, kept for someone to check (see docs/RUNBOOK.md).
CREATE TABLE IF NOT EXISTS dead_letters (
    id           BIGSERIAL    PRIMARY KEY,
    topic        VARCHAR(100) NOT NULL,
    partition_no INT          NOT NULL,
    offset_no    BIGINT       NOT NULL,
    reason       TEXT         NOT NULL,
    payload      TEXT         NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    UNIQUE (topic, partition_no, offset_no)
);
