# Developer - 3 - Documentation Writing Rules

This page defines how Fluxon user docs, developer docs, and design docs should be written. The goals are simple:

- Let readers reach the stable conclusion as quickly as possible.
- Keep docs aligned with code, public contracts, and actual behavior.

## 1. General Rules

- Lead with the conclusion, then expand. A reader should know what the page answers within the first 30 seconds.
- Prefer natural engineering terms. Avoid invented or template-heavy wording such as `root object`, `first-level branch`, or `authority object` unless the term is truly necessary.
- Write for readers, not as a dump of the author's thinking process. Remove template filler such as "this section does not discuss" or "why this branch belongs to the previous layer."
- If a paragraph can become a table, list, or diagram, do that instead of forcing a long linear explanation.
- API names, type signatures, and return semantics in docs must match the code exactly.
- Examples and quick starts must use the stable public contract. Do not expose internal compatibility layers, probing logic, or legacy usage in user-facing paths.
- Performance claims must be scoped. State the workload, baseline, and boundary of the conclusion instead of writing abstract claims like "faster."
- Rules should be reusable methods, not a retelling of the current case. Concrete technical facts, incident lessons, and benchmark numbers belong in examples, counterexamples, or review checklists.
- For behavior, ownership, or performance claims, define the observation scope first: abstraction level, covered path, preconditions, and exclusions.
- Do not lift a local fact into a system-level conclusion without tracing the full path at the same abstraction level.
- If the opening enumerates pain points, goals, evaluation dimensions, or explicit questions, the later sections should answer against that same list. Do not open on one axis and expand on another.
- Keep one canonical name, spelling, and capitalization for one concept across a page and across the doc set. Roles, component names, and acronyms should stay consistent.

## 2. Design Docs

Design docs should default to an RFC or ADR style. Prefer this main structure:

1. Background and goals
2. Non-goals
3. Core modules or roles
4. Architecture diagram or sequence diagram
5. Detailed design by dataflow or call flow
6. Constraints, invariants, and failure conditions
7. Key conclusions

Design docs should especially follow these rules:

- For cross-language boundaries, lifetime management, async sequencing, or ownership transfer, add a diagram by default. Readers rarely build a stable model from text alone.
- Organize by how data moves, not by the author's internal tree structure.
- Separate public contracts, current implementation, and specialized fast paths explicitly.
- For conclusions that are easy to overgeneralize, verify the full chain at one abstraction level: how input is formed, how the boundary is crossed, how downstream consumes it, and how the lifetime ends.
- If multiple paths exist, such as a `put` pointer path and an `rpc_call(payload)` bytes path, compare them side by side instead of switching back and forth inside the prose.
- If one path only optimizes part of the operation, mark the remaining steps explicitly so readers do not mistake a local optimization for a whole-path result.

## 3. User Docs

- Lead with the stable usage pattern, then explain constraints and common pitfalls.
- Default to answering "how to use it," not "how it works internally."
- Commands, arguments, return objects, and prerequisites must be directly actionable.
- Do not introduce internal reserved fields, temporary adapters, or statements that are only true for the current implementation details.
- If an interface has a strong contract, such as `FlatDict`, `MemHolder`, or `Future.wait()`, the doc should use that contract directly instead of suggesting loosely typed alternatives.
- Before a screenshot, GIF, terminal transcript, or Web UI preview, state the expected outcome or success condition in one line so the reader knows what they are about to see.

## 4. Developer Docs

- Only include what maintainers actually need: entry scripts, key directories, artifacts, commands, and rerun conditions.
- For process docs, prefer `command + artifact + when to rerun`.
- For code-tour docs, prefer `module responsibility + boundary + invariant`.
- Do not put one-off debugging notes, temporary operator steps, or personal environment paths into long-lived docs.

## 5. Expression Rules

- Avoid empty claims such as "improves maintainability" or "improves extensibility" unless you also name the object and mechanism.
- Avoid `not ... but ...` by default. Use it only when both sides are already established in context and the contrast materially helps at that exact spot.
- Avoid overly long paragraphs. Large blocks of prose usually want to become a diagram, table, or list.
- When a term first appears, anchor it to a real code module, struct, public type, or reserved field.
- For important boundaries, prefer tables that state `supported / not supported / why`.
- In Chinese or mixed-script docs, insert a half-width space between Chinese and Latin letters or digits by default. Do not add mechanical spaces around Chinese punctuation.
- Wrap exact identifiers such as commands, paths, ports, config keys, type names, API names, and third-party component names in backticks.
- If a list item contains both a takeaway and an explanation, prefer `**lead phrase**: explanation` so the reader can scan the conclusion first.
- Keep English list headings grammatically parallel. For pain points, capabilities, or trade-offs, noun phrases are usually the most stable shape.
- In English docs, prefer domain-idiomatic terminology and collocations over literal translation from Chinese source text.
- Acronyms should normally stay fully capitalized unless a brand, project, or public API defines a different spelling.
- For benchmarks, performance comparisons, and runtime observations, prefer objective engineering phrasing such as "roughly on par" or "still has room for further optimization" over conversational judgment.
- If one sentence carries multiple actions, turns, and conclusions, split it before the reader has to backtrack for the subject or constraint.

## 6. Examples and Counterexamples

The items below are examples and counterexamples only, not the rule itself. They exist to show why the rules matter:

- Writing "zero-copy for the whole call chain" when the system is only zero-copy across one language boundary.
- Writing "multi-part protocol payloads are naturally zero-copy" when only a single field is borrowable.
- Writing "the underlying buffer is reusable" as if upper-layer object construction had no cost.
- Writing "no extra memcpy on the data plane" as if the control plane, scheduling, locks, or the GIL also had no cost.

The common failure behind these mistakes is:

- The scope of the conclusion was never defined first.
- Facts from different abstraction levels were mixed together.
- A local observation was used in place of a full end-to-end argument.

## 7. Pre-Publish Checklist

Before landing a doc, check at least these items:

- The opening already states the goal and scope of the page.
- API names, paths, and type names in the text really exist.
- Public contracts and internal specialized paths are not mixed together.
- A local optimization is not being presented as an end-to-end conclusion.
- Remaining costs such as encoding, assembly, materialization, locks, GIL overhead, or downstream reconstruction are not silently omitted.
- A diagram or table has not been omitted where it would clearly reduce reader effort.
- The writing does not contain obvious template tone, filler, or unnatural terms.
- There are no repeated paragraphs, duplicated sections, or pasted draft leftovers.
- Mixed-script spacing, backticks, terminology spelling, and capitalization are consistent.
- Table-of-contents links, anchors, image paths, and external links all resolve correctly.
- Code fences declare the right language, such as `bash`, `python`, or `yaml`.
