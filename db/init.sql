CREATE TABLE trades (
  trade_id    UUID PRIMARY KEY,                -- the idempotency key
  account_id  TEXT NOT NULL,
  symbol      TEXT NOT NULL,
  side        TEXT NOT NULL CHECK (side IN ('BUY','SELL')),
  quantity    NUMERIC(18,4) NOT NULL CHECK (quantity > 0),
  price       NUMERIC(18,6) NOT NULL CHECK (price > 0),
  executed_at TIMESTAMPTZ NOT NULL,
  received_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE positions (
  account_id   TEXT NOT NULL,
  symbol       TEXT NOT NULL,
  net_quantity NUMERIC(18,4) NOT NULL,
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (account_id, symbol)
);
