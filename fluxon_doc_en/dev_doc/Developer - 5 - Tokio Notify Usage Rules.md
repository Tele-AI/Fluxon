# Developer - 5 - Tokio Notify Usage Rules

For in-process asynchronous state waiting, the persistent state is authoritative and `tokio::sync::Notify` is only a wake-up hint. Ordinary Fluxon call sites must use `fluxon_util::notify_state` so the race-safe protocol has one implementation:

1. Publish the state transition before sending the notification.
2. Use `notify_state::wait_until` for a predicate-only wait or `notify_state::wait_until_or_stopped` for a stop-aware wait.
3. Keep a custom loop only when the call site adds a real contract such as blocker accounting or a diagnostic timer.
4. Custom loops must retain `check -> arm -> recheck -> wait` and keep notification, shutdown, and timers in one cancellable `tokio::select!`.

This protocol closes the lost-wakeup window without treating notifications as durable events.

## 1. Scope and Primitive Choice

These rules cover process-local Rust code where a task waits for a persistent predicate such as `ready`, `closed`, a generation change, or a sequence becoming current. They do not define inter-process delivery, durable queues, or distributed coordination.

Choose the primitive from the required contract:

| Required contract | Preferred Tokio primitive |
| --- | --- |
| A persistent predicate may have changed; coalescing is acceptable | State plus `Notify` using this page's protocol |
| Every item must be consumed once | `mpsc` |
| Active subscribers need each item and can handle lag | `broadcast` |
| Subscribers need the latest value or version | `watch` |
| Permits must be counted | `Semaphore` |
| Exactly one result is delivered once | `oneshot` |

`Notify` is not an event log. `notify_one()` can retain at most one permit, while repeated notifications may coalesce. `notify_waiters()` does not provide durable delivery to future waiters. If each occurrence matters, use a primitive that stores items or permits.

## 2. Required Invariants

| Invariant | Required rule | Failure prevented |
| --- | --- | --- |
| State is authoritative | A waiter returns only after reading the predicate or terminal state. | A notification being mistaken for proof that the condition is true. |
| State precedes wake-up | Update the atomic or lock-protected state, release the lock if present, then notify. | A waiter waking, observing stale state, and sleeping after the only notification. |
| The registration gap is closed | Check, create and arm `Notified`, then recheck before awaiting. | A transition occurring between the first check and the wait. |
| Waiting remains cancellable | Put shutdown and notification in the same `select!`. | Shutdown being unable to interrupt an earlier await. |
| Notifications may be spurious or coalesced | Always loop after a wake-up and evaluate state again. | Progress depending on one notification per transition. |
| Locks do not cross suspension points | Drop synchronous mutex guards before `.await`. | Deadlock and executor starvation. |

## 3. Canonical Utility

The publisher commits the state first:

```rust
fn mark_ready(&self) {
    self.ready.store(true, Ordering::SeqCst);
    self.changed.notify_waiters();
}
```

For lock-protected state, mutate the value inside the lock, drop the guard, and then call `notify_one()` or `notify_waiters()`.

The predicate passed to the utility must synchronously read persistent state, have no side effects, and remain safe when called repeatedly. An asynchronous predicate or custom per-wake accounting requires a specialized loop.

For a predicate-only wait, call the utility directly:

```rust
use fluxon_util::notify_state;

notify_state::wait_until(&self.changed, || {
    self.ready.load(Ordering::SeqCst)
})
.await;
```

For a stop-aware wait, use the finite outcome contract:

```rust
use fluxon_util::notify_state::NotifyStateWaitOutcome;

match notify_state::wait_until_or_stopped(&self.changed, shutdown, || {
    self.ready.load(Ordering::SeqCst)
})
.await
{
    NotifyStateWaitOutcome::Ready => handle_ready(),
    NotifyStateWaitOutcome::Stopped => handle_closed(),
}
```

Do not add a method that only renames one of these calls and returns its result unchanged. A domain method is useful when it validates inputs, maps the outcome into a domain error, adds diagnostics, or owns another lifecycle boundary.

The utility internally owns the fixed `check -> arm -> recheck -> wait` loop. Ordinary call sites must not copy it. `Notified::enable()` is the canonical non-awaiting way to arm a pinned notification future. It is especially important when `notify_one()` is used with multiple waiters. The second predicate check remains mandatory because it also covers a transition that completed before the future was armed.

```mermaid
flowchart TD
    A[Utility checks stop and state predicate] -->|terminal or ready| Z[Return outcome]
    A -->|still waiting| B[Create and pin Notified]
    B --> C[Call enable]
    C --> D[Recheck stop and predicate]
    D -->|terminal or ready| Z
    D -->|still waiting| E[Select stop and notification]
    E --> A
```

The two checks and the armed waiter cover every transition window:

| Transition timing | How progress is observed |
| --- | --- |
| Before the first check | The first check sees the state. |
| Between the first check and arming | The second check sees the state. |
| Between arming and the second check | The second check sees the state; the notification may also be ready. |
| After the second check | The armed notification wakes the waiter. |
| At the same time as shutdown | The terminal branch wins when the documented contract uses biased terminal priority. |

## 4. Prohibited Patterns

### 4.1 Check Once and Await

```rust
if !self.is_ready() {
    self.changed.notified().await;
}
```

A transition can occur after `is_ready()` and before the notification future can observe it. A loop and the second state check are required.

### 4.2 Treat `select!` `else` as a Non-Blocking Default

```rust
tokio::select! {
    _ = &mut notified => {}
    else => {}
}
```

The `else` branch runs only when all branches are disabled. A pending notification branch is enabled, so this code waits. Use `Notified::enable()` to arm the future without awaiting it.

### 4.3 Await Notification Before Adding Cancellation

Do not perform an initial notification await and add shutdown only in a later `select!`. Notification, shutdown, timeout, and diagnostic wake-ups that must interrupt one another belong to the same wait set.

### 4.4 Notify Before Publishing State

Do not send the wake-up and then update the predicate. The publisher must make the new state visible first.

### 4.5 Use `Notify` as an Event Counter

Do not infer an event count from the number of wake-ups. Use `mpsc`, `broadcast`, or `Semaphore` when occurrences must be retained.

## 5. Shutdown Priority and Memory Visibility

- **Terminal priority**: when shutdown must win over normal progress, use `tokio::select! { biased; ... }` and place the terminal branch first. If normal progress may win, document that arbitration explicitly.
- **Diagnostic timers**: warning or telemetry timers may share the same `select!`; after they fire, the loop must recheck state. A diagnostic timeout must not silently become success.
- **Wake cardinality**: use `notify_waiters()` when one transition can satisfy or terminate every waiter. Use `notify_one()` only when exactly one waiter should compete to make progress.
- **Mutex state**: write while holding the mutex, release the guard, and then notify.
- **Atomic state**: use an ordering that establishes visibility between the publisher and waiter, such as release/acquire or `SeqCst`. `Relaxed` requires a separate, documented synchronization proof.
- **Stop implementation**: `AsyncStopSignal::is_stopped` is authoritative. `wait_stopped` must be cancellation-safe because `select!` may drop it when another branch wins.
- **Repeated close**: terminal state should be idempotent. A waiter starting after close must return from the state check without requiring another notification.

## 6. Required Tests

The shared utility must have bounded tests for the rows below. Ordinary call sites test their business predicate and outcome mapping. Any specialized custom wait helper must also cover every applicable interleaving:

| Case | Required assertion |
| --- | --- |
| State is ready before waiting | Returns immediately. |
| Terminal state is set before waiting | Returns the terminal outcome immediately. |
| State changes between the first check and waiter arming | Completes without a lost wake-up; use a deterministic test hook or barrier. |
| State changes after waiter arming | The notification wakes the task. |
| Shutdown occurs while waiting | Returns within a short test deadline. |
| State change and shutdown become ready together | Follows the documented priority. |
| Multiple broadcast waiters | Every waiter observes the persistent state. |
| Spurious notification | Rechecks the predicate and continues waiting. |
| Repeated terminal signal | Remains idempotent and observable. |

Wrap async wait tests in `tokio::time::timeout` so a regression becomes a bounded failure instead of hanging the suite. Stress tests may supplement these cases, but they do not replace deterministic interleaving tests.

## 7. Review Checklist and Reference Implementations

Before approving code that uses `Notify`, verify:

- An ordinary synchronous-predicate wait uses `fluxon_util::notify_state` instead of copying the loop.
- The predicate or terminal flag is persistent and has one authoritative storage location.
- The publisher updates state before notifying.
- The waiter follows `check -> arm -> recheck -> wait` inside a loop.
- Shutdown and notification are in one cancellable wait set.
- Simultaneous readiness has an explicit priority contract.
- No synchronous lock guard survives across `.await`.
- The required bounded interleaving tests exist.
- A channel, `watch`, or `Semaphore` was chosen instead if events or permits must be retained.

Current reference implementations and tests are:

- `fluxon_rs/fluxon_util/src/notify_state.rs`: canonical utility, stop contract, and interleaving tests
- `fluxon_rs/fluxon_mq/src/shutdown.rs`: `ShutdownCtl::wait_closed` as a utility call site
- `fluxon_rs/fluxon_framework/src/framework.rs`: `ResourceLatch::wait_ready` as a utility call site
- `fluxon_rs/fluxon_mq/src/consumer.rs`: `CommitSequencer::wait_turn` as a specialized wait with blocker diagnostics and a warning timer

These paths are the current references. Other existing `Notify` usages are not implicitly validated; check them against this protocol when they are modified.

API semantics are defined by [`tokio::sync::Notify`](https://docs.rs/tokio/latest/tokio/sync/struct.Notify.html) and [`tokio::sync::futures::Notified`](https://docs.rs/tokio/latest/tokio/sync/futures/struct.Notified.html).
