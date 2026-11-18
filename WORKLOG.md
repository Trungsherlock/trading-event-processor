# Work Log

Design decisions, tradeoffs, metrics and workflow for the trade event processor. Installs and code changes are tracked in git history, not here.

## Status

| Phase | State |
|---|---|
| 0. Setup | Done (2026-11-17) |
| 1. Learn C# basics | Self-study (no code) |
| 2. Docker Compose + schema | Done (2026-11-17) |
| 3. Domain + validation (TDD) | Done (2026-11-17) |
| 4. Repository + integration tests | Done (2026-11-17) |
| 5. Handler + Kafka consumer | Done (2026-11-18) |
| 6. Producer, crash test, throughput | Next |
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
| Integration tests share one Postgres container and empty the tables before each test | The container starts once per run instead of once per test. Every test still starts from an empty database. | Tests in the shared group run one at a time, not in parallel. |
| The handler splits failures into permanent and temporary | Permanent failures (malformed, invalid, rejected by the database) go to the DLQ and the offset is committed. Temporary ones (database or Kafka unreachable) propagate, and the offset isn't committed. | Every new failure type has to be classified, and a wrong guess either loops forever or loses a retry. |
| Retry the whole handler call in the worker: 3 retries with exponential backoff, then stop with exit code 1 | One retry point covers both database and DLQ outages. Stopping keeps the message uncommitted, so it's redelivered after a restart. | Something has to restart the worker. A retried DLQ publish can produce a duplicate DLQ entry. |
| Dead-letter queue (DLQ) for malformed or invalid messages | One bad message (a "poison pill") would otherwise block its partition forever. | Someone has to monitor the DLQ and replay or discard what lands there. |
| Plain ADO.NET, no ORM | Full control over the SQL (`ON CONFLICT`, the transaction boundary). No ORM overhead. | More boilerplate. Mapping is done by hand. |
| Core project has no Kafka or Postgres dependencies | Most tests run in milliseconds without Docker, which makes test-first practical. | Needs extra interfaces (`ITradeRepository`, `IDeadLetterPublisher`) and fakes. |
| Validation as a list of independent rules that collects every error | A new rule is a new class and the validator doesn't change. DLQ entries list everything that was wrong. | Slightly more classes than a single validation method. |
| Validation checks that quantity and price fit their `NUMERIC` columns | A value that's too large makes the insert fail every time, which would turn it into a poison pill. A value with too many decimals would be rounded silently. Both now go to the DLQ. | The rules duplicate the column definitions, so changing a column means updating its rule too. |
| `decimal` for quantity and price | `double` can't represent values like 0.1 exactly, so rounding errors add up. | Slower than `double`. That doesn't matter at this scale. |
| .NET 10 (LTS) over .NET 11 (STS, released 2026-11-10) | LTS is what companies run in production. Libraries have had a year of releases against it. | Misses C# 15 features, which this project doesn't need. Support ends at about the same time (Nov 2028) for both. |

## Open design issues

Found while reviewing the plan. Each one needs resolving in the phase listed.

1. ~~**Poison pills that get past the DLQ (Phases 3–5).**~~ Fixed: database values in Phase 3, parsing in Phase 5.
   - **Parsing:** a parse failure that isn't a `JsonException` or `FormatException` isn't caught, so the worker crashes and the message is redelivered forever. Examples: a `null` message body, an empty (tombstone) message, an unknown `side` value.
   - **Database:** a value too large for `NUMERIC(18,4)` passes validation but fails in Postgres. All DB errors are treated as temporary, so this also loops.
   - **Fix:** have `TradeMessage.Parse` turn every failure into one exception type (Phase 5, still open). The database part is fixed: the two precision rules added in Phase 3 reject these values before the insert.
2. ~~**The retry policy isn't in the code (Phase 5).**~~ Fixed in Phase 5. The failure table says "retry 3× with backoff, then crash", but nothing retries yet. The crash part already works, because an unhandled exception in a background service stops the host.
3. ~~**Each integration test starts its own Postgres container (Phase 4).**~~ Fixed in Phase 4: one container is shared, and the tables are emptied before each test.
4. ~~**A position can overflow even when each trade is valid (Phase 5).**~~ Fixed in Phase 5. Two trades of 99,999,999,999,999 each fit `trades.quantity`, but their sum doesn't fit `positions.net_quantity`. The transaction rolls back, which is correct, but the error is permanent, so the message would be redelivered forever. Fix: in the handler, treat Postgres data errors (codes starting with 22 or 23) as permanent and send the message to the DLQ. Keep retrying connection and timeout errors.

## Metrics

Fill these in only with measured values.

| Metric | Value | How it's measured | Date |
|---|---|---|---|
| Unit tests | 74 (local run; CI not set up yet) | `dotnet test` count | 2026-11-18 |
| Integration tests | 6 (local run; CI not set up yet) | `dotnet test` count | 2026-11-17 |
| Throughput, commit per message | — msg/s | 100k messages, local machine | |
| Throughput, background offset commit | — msg/s | 100k messages, local machine | |
| Crash test: trade count matches producer | — | `kill` during a 50k run, then compare with the producer's totals | |
| Crash test: positions match producer | — | Same as above | |

## Workflow

- **Phase gates:** each phase has a "Done when" check. The phase isn't finished until that check passes.
- **Schema changes:** `db/init.sql` runs only when the database is empty. After editing it, reset with `docker compose down -v`, then `docker compose up -d`.
- **Test-first:** the failing test and the code that makes it pass go in separate commits (`test: …` then `feat: …`), so the git history shows the order.
- **Measure, don't estimate:** no metric goes into this log or the README until it has been measured.
- **Compare options before choosing:** for each real design choice, list 2–4 options with their tradeoffs, pick one, and say why. Record it under "Options considered" in that phase's log entry.
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

#### Options considered

**How the Kafka topics get created**

| Option | Upside | Downside |
|---|---|---|
| A. Run `kafka-topics.sh` by hand (the plan) | No extra config | Easy to forget, and the README needs extra steps. If forgotten, the broker auto-creates `trades` with 1 partition. |
| B. Broker auto-create with a default of 3 partitions | Nothing to set up | `trades.dlq` also gets 3 partitions, and a typo silently creates a new topic. |
| **C. One-shot `kafka-init` container, auto-create off** ✅ | One command, the same result every time, and a typo fails loudly | Needs explicit listener config for the broker |
| D. The worker creates topics at startup (Kafka admin API) | Self-contained | The worker needs admin rights, the topic layout is hidden in code, and it races with the producer |

Chose C: the environment is defined in one file and can't drift into a 1-partition setup.

**Postgres port (5432 already used by a local install)**

| Option | Upside | Downside |
|---|---|---|
| A. Stop the local Postgres and use 5432 | Matches the plan | Disrupts the other install, and it has to be stopped every time |
| B. Use the local install instead of a container | No container | Shared with other projects. Its version may not match the tests' `postgres:17`. Setup and reset are manual. |
| **C. Container on host port 5433** ✅ | No conflict, same version as the tests, reset with one command | The connection string differs from the plan |

Chose C: it's isolated and reproducible. The only cost is one port number to remember.

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

#### Options considered

**Quantities and prices that don't fit their `NUMERIC` columns**

| Option | Upside | Downside |
|---|---|---|
| A. Let the insert fail, and send Postgres data errors (codes 22/23) to the DLQ | The schema is the only place the limits are defined | Caught late, after a database round trip. It also misses extra decimals, which Postgres **rounds silently** instead of rejecting. |
| **B. Validation rules that check size and decimal places** ✅ | Rejected before the database, catches silent rounding, and the error is listed with the other validation errors | The limits are written down twice (schema and rules) |
| C. Unlimited `NUMERIC` columns | Nothing can overflow | No limit on bad values, and `positions` could still grow without bound |

Chose B, because only validation catches silent rounding. Option A is still needed as a backstop for errors validation can't see, like a position that overflows (open issue #4).

**How tests control "now"**

| Option | Upside | Downside |
|---|---|---|
| A. Read the system clock directly | Simplest | Boundary cases like "exactly now" can't be tested reliably |
| B. `FakeTimeProvider` NuGet package | Full-featured (timers, advancing time) | A dependency for one method |
| **C. Hand-written fixed clock** ✅ | 3 lines, no package | Only returns a fixed time, which is all this test needs |

Chose C: it's enough for one rule and keeps the unit tests free of extra packages.

### 2026-11-17: Phase 4 (repository + integration tests)

- Repository: one transaction per trade. The trade insert is idempotent on `trade_id`, and the position is only updated when the insert actually added a row. All SQL is parameterized.
- Integration tests against real Postgres (Testcontainers, same schema file as Docker Compose) prove:
  - **The same trade saved twice is counted once.** This is the main claim of the project.
  - **8 simultaneous saves of one trade produce exactly one insert.** That's the case where two consumers handle the same message during a rebalance. Postgres makes the later inserts wait for the first one's commit, then skips them.
  - **Every field is stored exactly,** including 4- and 6-decimal values and a timestamp with a non-UTC offset.
  - **Buys and sells net out** separately for each account and symbol.
  - **A failed position update rolls back the trade insert too,** so trades and positions can't disagree. This test found open issue #4.
- Test setup: one shared container, with the tables emptied before each test. This closes open issue #3.
- Measured: the full local test run (46 tests, including the container start) takes about 15 s.
- Gate passed: the duplicate test passes against real Postgres. All 46 tests pass and the build has 0 warnings.

#### Options considered

**Postgres container lifetime in tests**

| Option | Upside | Downside |
|---|---|---|
| A. One container per test (the plan) | Full isolation | Every test pays several seconds of startup |
| B. One container per test class | Fewer starts | Still one per class, so it gets slower as test classes are added |
| **C. One container per run, tables emptied before each test** ✅ | One startup for the whole run | Tests in the group run one at a time, and each test must start by resetting |
| D. Point tests at the Docker Compose Postgres | No Testcontainers | Depends on a manually started environment, leftover data, and Compose in CI |

Chose C: it's the fastest option that still gives every test a clean database, and it works the same in CI.

**Sharing `TradeBuilder` between the test projects**

| Option | Upside | Downside |
|---|---|---|
| A. Copy the file | Simple | The two copies drift apart |
| B. A shared test-support project | Clean, and scales to more helpers | An extra project for one file |
| **C. Compile the unit tests' file into the integration tests too** ✅ | One copy, no new project | Its namespace says `UnitTests` |

Chose C for now. Switch to B if more helpers need sharing.

### 2026-11-18: Phase 5 (handler + Kafka consumer)

- **Message flow:** parse, validate, store. Each message ends up stored, a duplicate, or in the DLQ, and only then is its offset committed. Anything else stops the worker without committing.
- **Parsing:** every way a message can be broken produces one exception type, which goes to the DLQ. That includes empty messages, JSON `null`, wrong types, unknown sides and missing fields (all listed by name). This closes open issue #1.
- **Permanent database errors:** the repository translates Postgres data errors (codes 22/23) into a "rejected" error that goes to the DLQ. This closes open issue #4.
- **Retry:** 3 retries at 200, 400 and 800 ms, then the worker stops with exit code 1. This closes open issue #2.
- **DLQ entries:** the original message, every error, and the time it failed. The worker waits for the broker to confirm the DLQ entry is stored before committing the source offset.
- **End-to-end check** against the Docker Compose stack, with 12 messages in total. Every outcome matched:
  - Valid trades were stored, and buys and sells netted correctly (ACC-1 / AAPL: 100 − 30 + 5 + 10 = 85).
  - A resent trade was logged as a duplicate and the position didn't change.
  - The invalid trade went to the DLQ with both its errors listed.
  - Broken JSON went to the DLQ.
  - The message with missing fields went to the DLQ with all 6 field names listed.
  - The overflowing position went to the DLQ as "Rejected by database: 22003".
  - **Stopping Postgres:** 3 retries, then the worker stopped without committing. After Postgres and the worker restarted, the same trade was redelivered and stored.
  - A worker restart after a forced stop resumed at the next uncommitted offset.
- **Found during the check:** Windows PowerShell adds a UTF-8 byte order mark (BOM) when piping text to a program. The first test message was rejected as invalid JSON because of it. The parser now ignores one leading BOM, as RFC 8259 allows (see the options below).
- Gate passed: valid trades appear in Postgres and bad ones in `trades.dlq`. 80 tests pass and the build has 0 warnings.

#### Options considered

**Turning the JSON message into a `Trade`**

| Option | Upside | Downside |
|---|---|---|
| A. Read the JSON straight into `Trade` | Least code | Fails in several different ways, and a missing field quietly becomes a default value (for example, a missing `executedAt` becomes year 0001 and passes the future check) |
| **B. Read into an intermediate type where every field is optional, then convert; one exception type for every failure** ✅ | Missing fields are reported by name, and the handler catches exactly one exception | A second type that mirrors `Trade` |
| C. A `TryParse` that returns a result instead of throwing | No exceptions for expected failures | More code for little gain at this message volume |

**Where permanent database errors are recognized**

| Option | Upside | Downside |
|---|---|---|
| A. The handler inspects Postgres error codes | One place | Core would depend on Postgres |
| **B. The repository translates codes 22/23 into a Core "rejected" error** ✅ | Core stays independent of the database, and the rule is covered by a test against real Postgres | The repository needs to know which errors are permanent |
| C. Treat every database error as temporary | Simplest | A permanent error, like the overflow in open issue #4, is retried forever |

**Where the retry lives**

| Option | Upside | Downside |
|---|---|---|
| A. Polly / Microsoft.Extensions.Resilience | Battle-tested, with jitter and circuit breakers | A dependency for one 3-retry loop |
| B. Retry inside the repository | Close to the database call | Doesn't cover DLQ publish failures |
| **C. A small retry helper in Core, called by the worker around the whole handler call** ✅ | One place covers database and Kafka outages, it's unit-tested without waiting, and it adds no package | No jitter. A retried DLQ publish can produce a duplicate DLQ entry. |
| D. No retry: stop on the first error | Simplest | A brief network blip stops the worker |

**After the last retry fails**

| Option | Upside | Downside |
|---|---|---|
| **A. Stop with exit code 1, offset not committed** ✅ | Nothing is lost, the failure is obvious, and a supervisor can restart it | Something has to restart the worker |
| B. Keep retrying forever | Recovers without a restart | Hides an outage. If one message takes longer than Kafka's 5-minute poll limit, the consumer is removed from the group. |
| C. Pause the partition and keep consuming the others | Healthy partitions keep moving | More complex, and a database outage affects every partition anyway |

**Testing the Kafka consumer loop**

| Option | Upside | Downside |
|---|---|---|
| A. Automated test with Testcontainers.Kafka | Repeatable, and runs in CI | A new package and a slow container, to test about 20 lines of loop code |
| **B. Manual end-to-end check against the Docker Compose stack** ✅ | Exercises the real broker, database and failure paths, including a database outage | Not repeatable in CI |

Chose B for now. The logic that matters (handler, retry, repository) is covered by automated tests. The crash test in Phase 6 exercises the loop again.

**A leading byte order mark (BOM)**

| Option | Upside | Downside |
|---|---|---|
| A. Strict: the message goes to the DLQ | The sender's bug is visible | Valid trades are rejected over an invisible encoding artifact |
| **B. Ignore one leading BOM** ✅ | Allowed by RFC 8259, one line of code | The sender's bug goes unnoticed |
| C. Reject any message that isn't clean ASCII | Strictest | Breaks legitimate UTF-8 content |
