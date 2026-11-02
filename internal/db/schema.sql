-- Two services, two schemas. They share a Postgres instance only for
-- convenience: no statement or transaction ever touches both.

CREATE SCHEMA IF NOT EXISTS saga;
CREATE SCHEMA IF NOT EXISTS ledger;

-- Orchestrator -------------------------------------------------------------

CREATE TABLE IF NOT EXISTS saga.transfers (
    id           uuid PRIMARY KEY,
    idem_key     text   NOT NULL UNIQUE,
    request_hash bytea  NOT NULL,
    from_account bigint NOT NULL,
    to_account   bigint NOT NULL,
    amount       bigint NOT NULL CHECK (amount > 0),
    currency     text   NOT NULL,
    state        text   NOT NULL,
    attempt      int    NOT NULL,
    deadline     timestamptz NOT NULL,
    reason       text   NOT NULL DEFAULT '',
    version      int    NOT NULL,
    created_at   timestamptz NOT NULL,
    finished_at  timestamptz
);
CREATE INDEX IF NOT EXISTS transfers_open_by_deadline
    ON saga.transfers (deadline) WHERE finished_at IS NULL;

-- Append-only transition log; seq = transfers.version after the change.
CREATE TABLE IF NOT EXISTS saga.transitions (
    saga_id    uuid NOT NULL,
    seq        int  NOT NULL,
    from_state text NOT NULL,
    to_state   text NOT NULL,
    attempt    int  NOT NULL,
    at         timestamptz NOT NULL,
    PRIMARY KEY (saga_id, seq)
);

CREATE TABLE IF NOT EXISTS saga.outbox (
    id         bigserial PRIMARY KEY,
    topic      text NOT NULL,
    key        text NOT NULL,
    msg_id     text NOT NULL,
    payload    text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS saga.inbox (
    msg_id text PRIMARY KEY,
    at     timestamptz NOT NULL DEFAULT now()
);

-- Ledger -------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS ledger.accounts (
    id        bigint PRIMARY KEY CHECK (id > 0),
    available bigint  NOT NULL CHECK (available >= 0),
    held      bigint  NOT NULL CHECK (held >= 0),
    frozen    boolean NOT NULL DEFAULT false,
    currency  text    NOT NULL CHECK (currency ~ '^[A-Z]{3}$')
);

-- One row per (saga, step): the business-level idempotency key. Rejections
-- and fences are recorded too, so a duplicate replays the original answer.
-- An operation is in one currency; its entries carry none of their own, so
-- clearing and equity accounts hold several currencies without mixing them.
CREATE TABLE IF NOT EXISTS ledger.operations (
    id         bigserial PRIMARY KEY,
    saga_id    uuid   NOT NULL,
    step       text   NOT NULL,
    account_id bigint NOT NULL,
    amount     bigint NOT NULL,
    currency   text   NOT NULL,
    outcome    text   NOT NULL,
    reason     text   NOT NULL DEFAULT '',
    at         timestamptz NOT NULL DEFAULT now(),
    UNIQUE (saga_id, step)
);

CREATE TABLE IF NOT EXISTS ledger.entries (
    op_id      bigint NOT NULL REFERENCES ledger.operations (id),
    account_id bigint NOT NULL,
    bucket     text   NOT NULL CHECK (bucket IN ('available', 'held')),
    amount     bigint NOT NULL
);
CREATE INDEX IF NOT EXISTS entries_by_op ON ledger.entries (op_id);
-- Statements (GET /accounts/{id}/statement). Customer accounts only: the
-- clearing and equity accounts get a leg in almost every operation, and an
-- index on them would be a hot page for no reader.
CREATE INDEX IF NOT EXISTS entries_by_account ON ledger.entries (account_id, op_id) WHERE account_id > 0;

CREATE TABLE IF NOT EXISTS ledger.outbox (
    id         bigserial PRIMARY KEY,
    topic      text NOT NULL,
    key        text NOT NULL,
    msg_id     text NOT NULL,
    payload    text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE IF NOT EXISTS ledger.inbox (
    msg_id text PRIMARY KEY,
    at     timestamptz NOT NULL DEFAULT now()
);
