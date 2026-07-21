# Developer - 6 - Code Review Guidelines

A code-review rule turns an architecture conclusion into an executable constraint. “Responsibilities should be clear” or “lifecycle order should be correct” is only a conclusion; a complete rule also states when it applies, what must be done, how completion is accepted, what benefit it creates, and how to limit the fix's side effects.

## 1. Required structure of a rule

Every rule in this document contains these fields:

| Field | Question to answer |
| --- | --- |
| Trigger | Which code shape, dependency, or behavior change activates this rule? |
| Required action | Which constraint must the author and reviewer establish or change? |
| Acceptance criteria | Which observable postconditions mean the work is complete? |
| Expected benefit | Which risk is reduced or engineering capability is gained? |
| Side-effect boundary | Which extra complexity, coupling, or behavior change must the fix avoid? |
| Evidence | Which test, type constraint, or runtime result proves acceptance? |

“Must” is a pre-merge requirement. “Should” permits deviation with an explicit rationale in the PR. “May” is an implementation choice that does not affect merge.

## 2. Select rules by change scenario

The reviewer routes change signals to relevant rules instead of mechanically applying every rule to every PR.

| Change signal | Primary rules |
| --- | --- |
| A public layer starts accessing internal fields, or one module creates a resource and another cleans it up | R1 responsibility and ownership, R4 composable contracts, R5 minimal-side-effect fix |
| New `close`, background work, thread, process, runtime, or nested handle | R2 dependency lifecycle, R3 state authority |
| Multiple participants share `closed`, an event, a lease, or cleanup state | R1 responsibility and ownership, R3 state authority |
| New adapter, backend replacement, or parent wrapping a child | R1 responsibility and ownership, R4 composable contracts, R5 minimal-side-effect fix |
| Close races with an in-flight operation, or an outer lock spans an inner blocking call | R2 dependency lifecycle, R3 state authority |
| Source, configuration, docs, logs, screenshots, fixtures, or generated artifacts touch authentication material or real environment identifiers | R6 sensitive information and environment sanitization |
| A purely local algorithm change does not alter boundaries, resources, or lifecycle | Do not force a complete lifecycle model; review local correctness and tests |

## 3. Executable rules

### R1: Cross-module state and resources have one owner

| Field | Rule |
| --- | --- |
| Trigger | A change crosses public, composition, and internal layers; adds a shared resource; introduces duplicate close; or lets an outer layer mutate internal state. |
| Required action | List the creator, borrower, sharer, and final releaser of affected state and resources. Assign one final-release authority to each resource. The public layer expresses stable contracts, the composition layer orders modules, and the internal module maintains its state, invariants, and cleanup. |
| Acceptance criteria | Every resource answers “who releases last”; a borrower does not release owner resources; outer code does not manipulate internal fields to perform cleanup; replacing internals leaves public and composition lifecycle logic unchanged. |
| Expected benefit | Cohesive responsibility, no double release or leak, a smaller change-propagation radius, and independently testable and replaceable modules. |
| Side-effect boundary | Do not split stateless thin modules merely to create layers, expand public APIs, promote a local resource to global sharing, or move unrelated responsibilities. |
| Evidence | An ownership table or type relation, public contract tests, and tests proving borrower close preserves owner resources, exclusive-owner close completes release, and shared authority follows its declared release condition. |

### R2: Dependent lifecycles execute in partial order

| Field | Rule |
| --- | --- |
| Trigger | An object depends on a runtime, transport, store, worker, thread, process, or handle; or a change adds startup, shutdown, restart, or hot replacement. |
| Required action | Write the dependency DAG. If `A` depends on `B`, initialization satisfies `B → A` and destruction satisfies `A → B`. Shutdown establishes an admission barrier, propagates wake or cancel, converges in-flight work, joins background tasks, releases in reverse dependency order, and publishes completion. Lock hierarchy and callback direction remain compatible with dependency direction. |
| Acceptance criteria | Shutdown admits no new operations; all dependents are quiescent before a dependency releases; after close returns no task, callback, or handle inside that lifecycle boundary uses resources it owns; repeated close reaches the same terminal state. |
| Expected benefit | Deterministic lifecycle without use-after-close, shutdown deadlock, orphaned background work, or premature dependency release. |
| Side-effect boundary | Order only real dependencies; independent branches may close concurrently. Do not introduce a global lock or coordinator merely to unify order; wake or cancel remains scoped to resources owned by this shutdown. |
| Evidence | Real lifecycle tests, close racing with in-flight work, timeout tests, and completion-barrier tests showing no activity after close returns. |

### R3: Stop commands, in-progress state, and completion proof have explicit authorities

| Field | Rule |
| --- | --- |
| Trigger | Multiple objects read and write one boolean or event; stop request and release are asynchronous; close is retryable; or several layers can publish closed. |
| Required action | Assign every state a scope, sole writer, and meaning. Separate the stop command, `Closing` progress, and `Closed` completion proof. Prefer one monotonic state machine within one scope; signals and completion in different scopes remain owned by their respective modules. Name a signal-only API `request_shutdown()` or an equivalent explicit contract. |
| Acceptance criteria | Another participant's stop does not suppress this module's cleanup; only the owner publishes module completion; state advances only toward a terminal state; failure or retry neither reopens admission nor permanently skips unfinished work. |
| Expected benefit | No ambiguity in shared booleans, retryable and awaitable shutdown, better observability, and no premature return or resource leak. |
| Side-effect boundary | Do not add multiple sources of truth for one fact, introduce unnecessary states for a simple synchronous object, spread boolean combinations when a finite enum suffices, or keep old state aliases as compatibility branches. |
| Evidence | State-transition tests, tests where participants issue stop in different orders, and retry tests after partial construction or cleanup failure. |

### R4: Child lifecycle contracts compose directly in parents

| Field | Rule |
| --- | --- |
| Trigger | A parent wraps a child, an adapter or backend is added, one close invokes several children, or errors cross layers. |
| Required action | Define child lifecycle preconditions, success postconditions, and failure postconditions. Pair create / release, register / unregister, spawn / join, and subscribe / cancel. Parents invoke child contracts and order them without copying cleanup. Independent cleanup continues after partial failure, preserving the primary error and teardown errors. |
| Acceptance criteria | A parent decides the next step without inspecting child internals; child close provides the conditions required for reverse-order parent release; replacing a conforming implementation requires no parent change; failure reports which postconditions remain unsatisfied. |
| Expected benefit | Local correctness composes into system correctness, backends remain replaceable, failure causality survives, and modules evolve independently. |
| Side-effect boundary | Do not build a generic framework for hypothetical future implementations. Abstract only stable common semantics, keep specialized fast paths internal, and do not flatten all errors into an information-free result. |
| Evidence | Contract tests, consistency tests with an alternate implementation or test double, and tests proving remaining cleanup runs after one child close fails. |

### R5: Fix at the invariant authority and limit the change radius

| Field | Rule |
| --- | --- |
| Trigger | A proposed fix adds an outer conditional, clears internal fields directly, introduces a global flag, compatibility branch, or configuration entry to bypass an internal responsibility or lifecycle defect. |
| Required action | Locate the owner of the violated invariant and fix state, cleanup, or contract there; the composition layer changes only required ordering. State non-goals, remove workarounds superseded by the new contract, and limit regression testing to affected boundaries. |
| Acceptance criteria | Cross-boundary knowledge decreases or does not grow; no duplicate entrypoint, parallel configuration channel, or second lifecycle path is added; unrelated public behavior remains unchanged. |
| Expected benefit | Root-cause correction with a smaller regression radius and no accumulation of compatibility layers and conditional branches. |
| Side-effect boundary | Do not rewrite unrelated modules or expand migration under “architecture unification.” If a public contract must change, describe migration and impact separately instead of hiding it in an internal fix. |
| Evidence | A pre-fix counterexample, post-fix invariant tests, affected-caller regression, and review showing that the diff adds no bypass. |

### R6: Repositories and published artifacts do not expose sensitive information or private environment identifiers

| Field | Rule |
| --- | --- |
| Trigger | A change includes source, configuration, examples, docs, CI, logs, stack traces, screenshots, recordings, test fixtures, reports, or other generated artifacts that may carry data from a real personal environment or development, test, or production cluster, or may contain authentication material. |
| Required action | Identify and remove passwords, tokens, API keys, access keys, private keys, cookies, sessions, credential-bearing connection strings, and other authentication secrets. Sanitize real IP addresses, domains, hostnames, user names, node names, cluster IDs, port mappings, storage paths, and cluster topology unless publication is explicitly approved. Documentation and examples use reserved placeholders such as `example.com`, `example-node-a`, and the RFC 5737 ranges `192.0.2.0/24`, `198.51.100.0/24`, and `203.0.113.0/24`. Runtime secrets use the project's existing canonical secret or configuration mechanism. Logs, screenshots, and generated artifacts are sanitized before tracking or upload. |
| Acceptance criteria | The tracked diff, binary assets, and pending publication artifacts contain no valid or plausibly valid authentication secret and no unapproved real private-environment identifier. Errors and logs omit or mask sensitive values, and placeholders remain internally consistent within an example. If a valid secret entered a commit or published artifact, revoke or rotate it first and handle history and caches through the project's security process before approval. |
| Expected benefit | Prevent credential misuse and disclosure of private networks or cluster topology while keeping docs, tests, and diagnostic artifacts safe to share. |
| Side-effect boundary | Preserve diagnostic field names, error classes, and causal structure; use stable, correlatable fake values instead of erasing useful context. Explicitly approved public endpoints may remain. Do not create a parallel configuration channel for sanitization, and do not replace real values with values that merely look fictitious while remaining routable or authenticatable. |
| Evidence | Manually inspect the complete diff and every image or generated artifact, run the repository-approved secret scanner, and perform targeted checks for IPs, hostnames, paths, and log output. When redaction logic changes, add tests proving the raw sensitive value cannot reach output. |

## 4. Review output and merge criteria

A finding cites the rule and includes the trigger, violated acceptance criterion, impact, and side-effect boundary:

```text
[R3][blocking] A shared stop signal is treated as proof that this module completed cleanup.

Trigger: another participant sets the shared state first.
Violated criterion: cleanup owned by this module is skipped, so close is no longer a completion barrier.
Required action: separate shared stop intent from module-local completion, and let this module publish completion.
Side-effect boundary: do not add a second public close API or let the outer layer release internal resources directly.
```

| Label | Merge criterion |
| --- | --- |
| `[blocking]` | A plausible trigger violates acceptance criteria; fix before merge. |
| `[major]` | A critical scenario lacks a constraint or evidence; implement or verify before merge. |
| `[minor]` | A local maintainability or diagnostic issue does not affect acceptance criteria and may be explicitly deferred. |
| `[question]` | Trigger, authority, or contract is unclear; classify after clarification. |

R6 uses stricter severity: valid or plausibly valid credentials or private keys, and real IPs, hostnames, or cluster topology without publication approval, are always `[blocking]`. If permission to publish an environment identifier is unclear, label it `[question]` and do not approve until resolved.

- **Request changes**: an unresolved `[blocking]` or `[major]` remains, or the applicable rule's owner, dependency, or acceptance criteria cannot be determined.
- **Approve**: required actions for all matched rules are complete, evidence satisfies acceptance criteria, expected benefits hold, and side effects stay inside the declared boundary.
- **Comment**: only clarification questions or suggestions that do not affect acceptance remain.

Documentation PRs must also follow the [Documentation Writing Rules](./Developer%20-%203%20-%20Documentation%20Writing%20Rules.md) and [Technical Documentation Copy Editing](./Developer%20-%205%20-%20Technical%20Documentation%20Copy%20Editing.md).
