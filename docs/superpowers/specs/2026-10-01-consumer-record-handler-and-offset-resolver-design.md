# KafkaConsumerComponent: record handler + start-offset resolver

Date: 2026-10-01
Status: approved, pending implementation plan

## Problem

`KafkaConsumerComponent` currently only supports a decoded-value handler
(`handler(value)`) and hardcodes `auto_offset_reset="earliest"`. Applications
that activate a consumer at a business-defined moment (e.g. a notification
route created at 12:00) need two things the current API can't provide:

1. The exact Kafka coordinates (`topic`, `partition`, `offset`) of a record,
   so a durable failure record can be written before advancing past a bad
   message, and so malformed JSON can be reported without losing its source
   coordinates.
2. A way to seek a newly-assigned partition to an application-supplied
   absolute offset (the partition's end-offset at route-creation time) when
   the consumer group has no committed offset for it yet — `latest` misses
   records produced between route creation and consumer startup; `earliest`
   replays everything.

Full request: see the originating feature request (GitHub issue against
`fabriciooml/kafka-component`).

## Scope

In scope: an opt-in full-record handler mode, an opt-in per-partition
start-offset resolver invoked on first assignment (and on rebalance, for any
partition without a committed offset), and an `auto_offset_reset` passthrough
option. Existing `handler(value)` behavior is unchanged.

Out of scope (explicitly, per the originating request): database-backed
offset storage, dynamic route/topic CRUD, Telegram-specific delivery/retry,
exactly-once delivery.

## API

```python
@dataclass(frozen=True)
class KafkaRecord:
    topic: str
    partition: int
    offset: int
    value: bytes | None
    key: bytes | None = None
    timestamp: int | None = None
    headers: tuple[tuple[str, bytes], ...] = ()


@dataclass(frozen=True)
class PartitionAssignment:
    topic: str
    partition: int
    beginning_offset: int
    end_offset: int


class StartOffsetOutOfRangeError(Exception):
    def __init__(self, topic: str, partition: int, requested_offset: int,
                 beginning_offset: int, end_offset: int) -> None: ...
```

`KafkaConsumerComponent.__init__` gains:

```python
record_handler: Callable[[KafkaRecord], Awaitable[None]] | None = None,
start_offset_resolver: Callable[[PartitionAssignment], Awaitable[int]] | None = None,
auto_offset_reset: Literal["earliest", "latest", "none"] = "earliest",
```

Exactly one of `handler` / `record_handler` must be supplied; supplying
both or neither raises `ValueError` at construction. `start_offset_resolver`
may be combined with either handler mode — it is a rebalance-time hook
independent of how records are subsequently delivered.

All four new names (`KafkaRecord`, `PartitionAssignment`,
`StartOffsetOutOfRangeError`, plus the existing public surface) are exported
from `kafka_component/__init__.py`.

## Design

### Handler dispatch

`_consume_loop` branches once per message on which handler was configured:

- Legacy: unchanged — `await self._handler(msg.value)`.
- Record mode: build a `KafkaRecord` from the raw `aiokafka` `ConsumerRecord`
  (`topic`, `partition`, `offset`, `value` as raw `bytes`, `key`, `timestamp`,
  `headers`) and `await self._record_handler(record)`.

Commit and `ErrorPolicy` wiring are untouched in both branches: `commit()` is
called with no arguments after a successful handler call (committing the
subscription's current position, i.e. `record.offset + 1`), and
`ErrorPolicy.handle(msg, exc)` still receives the raw `ConsumerRecord` on
failure, exactly as today.

In record mode, `value_deserializer` is left unset on the underlying
`AIOKafkaConsumer`, so `msg.value` is raw `bytes` and a malformed-JSON value
never raises before reaching the handler — decoding, and deciding what to do
on a decode failure, is the application's job. This also means the
deserialization-failure fatal path that exists today for legacy mode does not
apply in record mode; failures surface however the application's
`record_handler` or `ErrorPolicy` chooses to treat them.

### Start-offset resolution

A new internal class, `_ResolverRebalanceListener(ConsumerRebalanceListener)`,
is constructed only when `start_offset_resolver` is provided:

```python
class _ResolverRebalanceListener(ConsumerRebalanceListener):
    def __init__(self, consumer, resolver, on_error): ...

    async def on_partitions_revoked(self, revoked):
        pass

    async def on_partitions_assigned(self, assigned):
        assigned = list(assigned)
        if not assigned:
            return
        self._consumer.pause(*assigned)  # see note below — required for correctness
        for tp in assigned:
            committed = await self._consumer.committed(tp)
            if committed is not None:
                self._consumer.resume(tp)
                continue  # aiokafka will fetch and use it normally
            beginning = (await self._consumer.beginning_offsets([tp]))[tp]
            end = (await self._consumer.end_offsets([tp]))[tp]
            assignment = PartitionAssignment(tp.topic, tp.partition, beginning, end)
            target = await self._resolver(assignment)
            if not beginning <= target <= end:
                exc = StartOffsetOutOfRangeError(tp.topic, tp.partition, target, beginning, end)
                self._on_error(exc)
                raise exc  # tp (and any tps after it) stay paused — never silently consumed
            self._consumer.seek(tp, target)
            self._consumer.resume(tp)
```

**Correction (found during the final whole-branch review, verified against a
real broker):** an earlier version of this design claimed `on_partitions_assigned`
is fully awaited "before the consumer resumes fetching," based on reading
`aiokafka/consumer/{consumer,fetcher,subscription_state}.py`. That claim is
false. `group_coordinator.py`'s `_on_join_complete` calls
`self._subscription.assign_from_subscribed(assignment.partitions())`
*before* it calls the listener; that call wakes the fetcher's own
independent asyncio task (via `subscription_state.py`'s assignment waiters),
which then begins establishing positions and fetching concurrently with the
listener coroutine — not sequenced after it. A `start_offset_resolver` with
any realistic latency (e.g. a database lookup) therefore races the fetcher:
records are fetched using `auto_offset_reset`'s fallback position, delivered
to the handler, and committed, before the listener's `seek()` ever runs.
Probing against a real broker showed a resolver with 500ms latency let
records before the target offset through every time.

**Fix:** `pause()` every newly-assigned partition immediately, before
resolving any of them, and `resume()` each only once it is actually safe to
fetch from — either it already has a committed offset (`aiokafka` will
establish its position normally) or `seek()` has placed it at the resolved
offset. `aiokafka`'s fetcher (`fetcher.py`) never returns records for a
paused partition regardless of what position it holds, so pausing closes the
race window entirely, independent of resolver latency. On an out-of-range
result, the offending partition (and any not yet reached in the loop) is
left paused — since `_resolver_error` makes the whole consumer fatal moments
later, those partitions never need to be unpaused.

This relies on these confirmed `aiokafka` behaviors (read from
`aiokafka/consumer/{consumer,fetcher,subscription_state,group_coordinator}.py`
in the installed `.venv`, and verified against a real broker):

- `on_partitions_assigned` may be a coroutine and is awaited by the
  coordinator, but a separate fetcher task may already be running
  concurrently with it (see correction above) — the listener's own
  `pause()`/`resume()` calls, not call ordering, are what make this safe.
- `AIOKafkaConsumer.seek(tp, offset)` sets `TopicPartitionState._position`
  directly, and `_update_fetch_positions` skips any partition that already
  `has_valid_position`. A `seek()` issued inside the listener still wins over
  both the committed-offset fetch and `auto_offset_reset` for that
  partition's *position* — pausing is what prevents records from being
  *delivered* before that position is in place.
- `pause()`/`resume()` take `*partitions` (variadic `TopicPartition`
  arguments) and are valid to call on a partition the instant it is in the
  current assignment — which it is by the time the listener runs, since
  `assign_from_subscribed()` already registered it.

Because the constructor can no longer pass topics positionally to
`AIOKafkaConsumer` (the constructor's positional-topics path calls
`subscribe()` without a listener), `start()` instead constructs the consumer
with no topics and calls `self._consumer.subscribe(topics=self._topics,
listener=listener_or_None)` before `await self._consumer.start()`. This is
unconditional (legacy mode just passes `listener=None`), keeping one code
path for both handler modes.

### auto_offset_reset

Plain passthrough: new constructor param, default `"earliest"` (matches
today's hardcoded value exactly — no behavior change for existing callers
who don't pass it), forwarded to `AIOKafkaConsumer(auto_offset_reset=...)`.
It only governs partitions that reach `aiokafka`'s own reset logic — i.e.
partitions with no committed offset *and* no resolver, or a resolver that
chose not to act (not a case this design allows — the resolver, if present,
always runs for any partition with no committed offset).

**`"none"` is rejected at construction when combined with
`start_offset_resolver`** (`ValueError`). `aiokafka` establishes each
assigned partition's initial position — including raising
`NoOffsetForPartitionError` under `"none"` when no committed offset exists —
independently of whether the partition is paused; pausing (see above) only
withholds *delivery* of already-fetched records, not the position-
establishment step itself. So `"none"` would raise before the resolver's
`seek()` ever has a chance to run, making the combination always broken.
Since the resolver already runs for every partition with no committed
offset regardless of `auto_offset_reset`, `"none"` has no useful meaning
once a resolver is configured — callers who want "never silently fall back"
get exactly that from the resolver + `StartOffsetOutOfRangeError` path
already.

### Error handling

- Bad construction (`handler`/`record_handler` both or neither, or
  `auto_offset_reset="none"` with `start_offset_resolver` set) → `ValueError`
  raised synchronously from `__init__`.
- `StartOffsetOutOfRangeError` raised inside the rebalance listener does
  **not** propagate out of `aiokafka`'s coordinator on its own: the installed
  `aiokafka==0.14.0`'s `_on_join_complete` (`group_coordinator.py`) wraps the
  listener call in its own `try/except Exception: log.exception(...)` and
  never touches the `_pending_exception`/`check_errors()` mechanism
  `getone()` consults — confirmed during Task 5's implementation, correcting
  this section's original (incorrect) claim that it propagated unassisted.
  Instead, `_ResolverRebalanceListener` is given a one-arg `on_error`
  callback (bound to `KafkaConsumerComponent._record_resolver_error`, which
  sets `self._resolver_error`) that it calls immediately before re-raising.
  `_consume_loop` checks `self._resolver_error` at the top of its `while`
  loop, inside the outer `try:`, and re-raises it there if set — reaching
  the loop's existing outer `except`, the same fatal path used today for
  deserialization failures in legacy mode: `last_error` is set, `connected`
  becomes `False`, the loop exits, and `shutdown()` remains safe to call.
  The listener still raises too (not just calling `on_error`), so
  `aiokafka`'s own `log.exception` diagnostic is preserved. The component
  does not retry or crash-loop, consistent with its documented "not a
  supervisor" stance.
- All other failure handling (handler exception → `ErrorPolicy.handle`,
  policy exception → logged via `last_error`, offset not committed) is
  unchanged in both handler modes.

## Testing

Unit (no broker): constructor rejects both/neither of `handler`/`record_handler`;
constructor accepts `start_offset_resolver` with either handler mode.

Integration (real Kafka via `testcontainers`, extending `test_consumer.py`):

1. Existing `handler(value)` tests pass unchanged.
2. `record_handler` sees correct `topic`/`partition`/`offset`/`value` across
   two partitions.
3. No committed offset: resolver returns `N`; records before `N` are skipped,
   `N` onward handled — **including when the resolver has realistic latency**
   (e.g. an `await asyncio.sleep(...)` standing in for a database lookup);
   this is what the pause/resume fix specifically guards against, so a test
   using a near-instant resolver does not actually exercise it.
4. Records produced between offset capture and consumer startup are handled
   (producer writes after the resolver's captured `end_offset`, before
   `component.start()`; those records still arrive).
5. After handling and committing, a restarted component resumes from the
   committed offset and does not invoke the resolver.
6. On rebalance: a partition with an existing committed offset is left alone
   (resolver not invoked for it); a partition added at runtime (via admin
   `create_partitions`) invokes the resolver.
7. Handler exception invokes the `ErrorPolicy`; if the policy *succeeds*, the
   record is still committed — this is correct, existing skip-and-advance
   semantics ("write a durable failure record, return successfully, advance
   past a permanently bad message" per the originating request), not a bug.
   Only a *raising* policy leaves the offset uncommitted. (An earlier version
   of this item and the matching test name said the failed record is "left
   uncommitted" unconditionally — that was imprecise; the test is named to
   reflect the real, correct behavior.)
8. Malformed JSON in record mode: raw bytes plus correct topic/partition/offset
   reach the handler; the handler's own decode failure doesn't lose
   coordinates.
9. Out-of-range resolver result raises `StartOffsetOutOfRangeError`, surfaces
   as `connected: False` / non-null `last_error`, and — now that the
   partition stays paused rather than falling through to
   `auto_offset_reset` — commits nothing for that partition either; a probe
   consumer in the same group sees no committed offset. Not a silent
   earliest/latest-and-commit fallback.

## Non-goals

Carried over from the originating request: no database-backed offset storage,
no dynamic topic/route CRUD, no Telegram-specific delivery/retry/failure
logic, no exactly-once delivery guarantees.
