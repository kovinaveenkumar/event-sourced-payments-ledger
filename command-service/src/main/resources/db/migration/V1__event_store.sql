-- Event log. Rows are only added, never changed, except setting published_at once sent to Kafka.
CREATE TABLE events (
    global_seq   BIGSERIAL    PRIMARY KEY,
    event_id     UUID         NOT NULL UNIQUE,
    account_id   UUID         NOT NULL,
    version      BIGINT       NOT NULL,
    event_type   VARCHAR(64)  NOT NULL,
    payload      JSONB        NOT NULL,
    occurred_at  TIMESTAMPTZ  NOT NULL,
    published_at TIMESTAMPTZ,
    CONSTRAINT events_account_version_uk UNIQUE (account_id, version)
);

-- This index only covers unsent events, so finding them stays fast even when the table is big.
CREATE INDEX events_unpublished_idx ON events (global_seq) WHERE published_at IS NULL;

CREATE TABLE idempotency_keys (
    idempotency_key VARCHAR(128) PRIMARY KEY,
    fingerprint     VARCHAR(64)  NOT NULL,
    result_id       UUID         NOT NULL,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);
