# Work Log

Design decisions, tradeoffs, metrics and workflow for the trade event processor. Installs and code changes are tracked in git history, not here.

## Status

| Phase | State |
|---|---|
| 0. Setup | Done (2026-11-17) |
| 1. Learn C# basics | Self-study (no code) |
| 2. Docker Compose + schema | Done (2026-11-17) |
| 3. Domain + validation (TDD) | Done (2026-11-17) |
| 4. Repository + integration tests | Next |
| 5. Handler + Kafka consumer | Not started |
| 6. Producer, crash test, throughput | Not started |
| 7. CI | Not started |
| 8. UML + README | Not started |

## Design decisions

| Decision | Why | Cost |
|---|---|---|
| At-least-once delivery + idempotent write (`ON CONFLICT (trade_id) DO NOTHING`) | Kafka transactions don't extend to Postgres, so exactly-once isn't available end to end. A replayed message becomes a no-op. | Duplicates are still processed and rejected. Every message needs a stable unique ID. |
| Commit the Kafka offset only after the DB commit or the DLQ publish | A crash at any point leads to a replay, never a lost trade. | A crash between the DB commit and the offset commit causes a replay, which the idempotent write absorbs. |
| Insert the trade and update the position in one DB transaction | The position can never disagree with the stored trades. | Each message takes one round-trip transaction, which limits throughput (to be measured in Phase 6). |
| Partition key = `accountId`, 3 partitions | Keeps each account's trades in order. | Ordering only holds within an account. Parallelism is capped at 3 consumers, and one busy account can overload its partition. |
| Topics created by a one-shot `kafka-init` container. Broker auto-create is turned off. | `docker compose up -d` gives a ready environment in one step. With auto-create on, the first producer or a typo in a topic name would create a 1-partition topic, which quietly removes per-account parallelism. | The broker needs explicit listener config (one for other containers, one for the host) instead of the image defaults. Changing the partition count means editing the compose file and recreating the topic. |
| Dead-letter queue (DLQ) for malformed or invalid messages | One bad message (a "poison pill") would otherwise block its partition forever. | Someone has to monitor the DLQ and replay or discard what lands there. |
| Plain ADO.NET, no ORM | Full control over the SQL (`ON CONFLICT`, the transaction boundary). No ORM overhead. | More boilerplate. Mapping is done by hand. |
| Core project has no Kafka or Postgres dependencies | Most tests run in milliseconds without Docker, which makes test-first practical. | Needs extra interfaces (`ITradeRepository`, `IDeadLetterPublisher`) and fakes. |
| Validation as a list of independent rules that collects every error | A new rule is a new class and the validator doesn't change. DLQ entries list everything that was wrong. | Slightly more classes than a single validation method. |
| Validation checks that quantity and price fit their `NUMERIC` columns | A value that's too large makes the insert fail every time, which would turn it into a poison pill. A value with too many decimals would be rounded silently. Both now go to the DLQ. | The rules duplicate the column definitions, so changing a column means updating its rule too. |
| `decimal` for quantity and price | `double` can't represent values like 0.1 exactly, so rounding errors add up. | Slower than `double`. That doesn't matter at this scale. |
| .NET 10 (LTS) over .NET 11 (STS, released 2026-11-10) | LTS is what companies run in production. Libraries have had a year of releases against it. | Misses C# 15 features, which this project doesn't need. Support ends at about the same time (Nov 2028) for both. |

## Open design issues

Found while reviewing the plan. Each one needs resolving in the phase listed.

1. **Poison pills that get past the DLQ (Phases 3–5).**
   - **Parsing:** a parse failure that isn't a `JsonException` or `FormatException` isn't caught, so the worker crashes and the message is redelivered forever. Examples: a `null` message body, an empty (tombstone) message, an unknown `side` value.
   - **Database:** a value too large for `NUMERIC(18,4)` passes validation but fails in Postgres. All DB errors are treated as temporary, so this also loops.
   - **Fix:** have `TradeMessage.Parse` turn every failure into one exception type (Phase 5, still open). The database part is fixed: the two precision rules added in Phase 3 reject these values before the insert.
2. **The retry policy isn't in the code (Phase 5).** The failure table says "retry 3× with backoff, then crash", but nothing retries yet. The crash part already works, because an unhandled exception in a background service stops the host.
3. **Each integration test starts its own Postgres container (Phase 4).** Share one container across tests and empty the tables between them.

## Metrics

Fill these in only with measured values.

| Metric | Value | How it's measured | Date |
|---|---|---|---|
| Unit tests | 40 (local run; CI not set up yet) | `dotnet test` count | 2026-11-17 |
| Integration tests | — | CI test count | |
| Throughput, commit per message | — msg/s | 100k messages, local machine | |
| Throughput, background offset commit | — msg/s | 100k messages, local machine | |
| Crash test: trade count matches producer | — | `kill` during a 50k run, then compare with the producer's totals | |
| Crash test: positions match producer | — | Same as above | |

## Workflow

- **Phase gates:** each phase has a "Done when" check. The phase isn't finished until that check passes.
- **Schema changes:** `db/init.sql` runs only when the database is empty. After editing it, reset with `docker compose down -v`, then `docker compose up -d`.
- **Test-first:** the failing test and the code that makes it pass go in separate commits (`test: …` then `feat: …`), so the git history shows the order.
- **Measure, don't estimate:** no metric goes into this log or the README until it has been measured.
- **This log:** add an entry at the end of each phase covering decisions made, tradeoffs, and anything measured.

## Log

### 2026-11-17: Phase 0 (setup)

- Reviewed the build plan and found the three open design issues above.
- Chose .NET 10 LTS after .NET 11 shipped on 2026-11-10 (see the design decisions table).
- Set up the layered structure: Core (domain, no infrastructure), Infrastructure (Postgres, Kafka), Worker (host), unit tests, integration tests, and the producer tool.
- Gate passed: the solution builds and the template tests pass.

### 2026-11-17: Phase 2 (local environment + schema)

- Local environment: a single-node Kafka broker (KRaft, no ZooKeeper) and Postgres, both in Docker Compose with health checks.
- Topics: `trades` has 3 partitions and `trades.dlq` has 1. A one-shot container creates them, and broker auto-create is off (see the design decisions table).
- Schema: `trades.trade_id` is the primary key and serves as the idempotency key. CHECK constraints reject a non-positive quantity or price, or a side that isn't BUY or SELL. `positions` is keyed by (account, symbol).
- **Postgres is on host port 5433, not 5432**, because a local PostgreSQL install already uses 5432. The worker's connection string in Phase 5 must use `Port=5433`. The integration tests aren't affected, because Testcontainers picks random ports.
- Gate passed:
  - `psql` lists `trades` and `positions`.
  - `kafka-topics --list` shows both topics with the right partition counts.
  - Auto-create is confirmed off.
  - Both ports are reachable from the host.
  - Running `docker compose up -d` again exits cleanly.

### 2026-11-17: Phase 3 (domain + validation)

- Domain: an immutable `Trade` record. A trade's signed quantity (+ for a buy, − for a sell) is the amount it changes the position by.
- Validation: 8 independent rules, each checking one thing. The validator runs all of them and collects every error.
  - The plan's 6: positive quantity, positive price, symbol format, account ID required, trade ID required, not in the future.
  - 2 new ones that check quantity and price fit their database columns. This closes the database half of open issue #1.
- Edge cases the tests cover:
  - Fields missing from the JSON (null).
  - A symbol with a trailing newline, which a regex ending in `$` would let through.
  - Timestamps in different time zones that are the same instant.
  - Trailing zeros, which don't count as extra decimal places.
- The time check reads "now" from an injected clock, so tests are deterministic.
- Workflow: all tests were written first and run red (the build failed), then the code was written until they passed. Nothing was committed, so the git history doesn't show the test-first order.
- Gate passed: 40 unit tests pass with no Kafka, database or Docker. The plan's target was about 15–20.
