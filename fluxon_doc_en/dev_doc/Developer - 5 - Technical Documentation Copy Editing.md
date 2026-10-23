# Developer - 5 - Technical Documentation Copy Editing

This document adapts the parts of the upstream `copy-editing` skill that are useful for technical writing into a Fluxon documentation review workflow. Use it after the technical facts and structure are established. It does not replace the repository documentation rules and cannot change public APIs, runtime behavior, or performance conclusions.

## 1. Source and scope

| Item | Details |
| --- | --- |
| Upstream project | [`coreyhaines31/marketingskills`](https://github.com/coreyhaines31/marketingskills) |
| Upstream skill | [`copy-editing`](https://github.com/coreyhaines31/marketingskills/tree/0ba2a7fafc7b0827a261bd518e87cbda18e6675f/skills/copy-editing) |
| Skill version | `2.0.0` |
| Pinned commit | `0ba2a7fafc7b0827a261bd518e87cbda18e6675f` |
| Upstream purpose | Edit existing marketing and conversion copy while preserving its core message and voice. |
| Purpose here | Adapt its clarity, voice, rationale, evidence, and specificity checks to Fluxon technical documentation. |
| License | MIT; the complete notice appears at the end of this document. |

Resolve conflicts in this order:

1. The current task, repository `AGENTS.md`, and the [documentation writing rules](./Developer%20-%203%20-%20Documentation%20Writing%20Rules.md).
2. Code, public contracts, tests, and verified runtime behavior.
3. This copy-editing workflow.

Smoother prose is not a reason to change technical meaning. If a fact is uncertain, an interface disagrees with the documentation, or the available evidence is insufficient, verify the implementation before deciding whether to change the documentation or the code.

## 2. Applying the seven upstream sweeps to technical documentation

The upstream skill divides editing into seven sweeps. Fluxon directly adopts four, adapts two, and skips one:

| Upstream sweep | Fluxon use | Decision |
| --- | --- | --- |
| Clarity | Check long sentences, references, undefined terms, missing context, and conclusions buried under qualifications. | Adopt. |
| Voice and Tone | Keep formality, role names, component names, and terminology consistent while using natural engineering language. | Adopt. |
| So What | Explain the reason, effect, and cost of an architecture choice; answer why the design works this way. | Adapt to engineering rationale; do not add marketing benefits. |
| Prove It | Support behavior and performance claims with code paths, type signatures, tests, metrics, or bounded experiments. | Adopt. |
| Specificity | State the scope, abstraction level, preconditions, failure conditions, and excluded paths. | Adopt. |
| Heightened Emotion | Amplify pain, anxiety, aspiration, or emotional impact. | Skip. Technical documentation prioritizes accuracy and verifiability. |
| Zero Risk | Add marketing CTAs, guarantees, and risk reversals. | Keep only operational preconditions, failure results, and next steps in user documentation. Do not add marketing CTAs. |

The upstream limits for English word counts, active voice, and short paragraphs are heuristics. Judge Chinese sentence length, code identifiers, and complex ownership relationships by readability instead of applying fixed thresholds mechanically.

## 3. Review workflow

### 3.1 Establish the technical facts first

Before editing, identify:

- The document type and target reader.
- The boundaries between the public contract, current implementation, and specialized fast paths.
- Whether key types, configuration keys, return values, and failure semantics match the code.
- The complete path covered by behavior, ownership, and performance claims.
- The Chinese or English counterpart that must be updated at the same time.

If these points are unresolved, perform a technical review first. Copy editing cannot replace implementation verification.

### 3.2 Sweep one: structure and clarity

First check whether a reader can answer three questions from the opening:

1. What problem does this document address?
2. What is the most important stable information?
3. In what order will the document develop the topic?

Then review each section:

- Does the heading state the section's decision or task directly?
- Does the section lead with stable information at its current abstraction level before fields, branches, and measurements?
- Does the body answer the same set of concerns introduced at the beginning?
- Is the same fact repeated in a role table, flow table, diagram legend, and summary?
- Does local implementation detail appear too early in the introduction or architecture overview?

### 3.3 Sweep two: voice and terminology

- Use one canonical name, spelling, and capitalization for each concept.
- Give role boundaries explicit subjects, such as “master maintains `route`” and “owner manages local SSD.”
- Remove labels such as “stable conclusions” or “current conclusions” when the content is a summary or capability state. Use “core points,” “current status,” or the concrete statement instead.
- Avoid template language, promotional language, and unsupported claims to “improve,” “enhance,” or “optimize.”
- Keep required English code terms. Do not translate public types or fields merely to make the prose look more localized.

### 3.4 Sweep three: rationale, evidence, and boundaries

Check four properties for every important claim:

| Property | Question to answer |
| --- | --- |
| Rationale | Why does this responsibility boundary, data direction, or lifetime exist? |
| Mechanism | Which object, field, or call path implements the behavior? |
| Scope | Which step, abstraction level, and branches does the claim cover? |
| Exclusions | Which costs, paths, recovery capabilities, or deployment conditions are outside the claim? |

Place the reason close to the architecture claim. For example, when the master schedules memory allocations, immediately explain that these allocations cover both final value replicas and temporary cross-owner transfer memory. Listing ownership alone is insufficient.

Bind performance claims to hardware, datasets, concurrency, output boundaries, measurement windows, and run counts. End-to-end logical payload bandwidth must not be described as raw SSD bandwidth.

### 3.5 Sweep four: contraction and deduplication

Resolve repetition in this order:

1. Keep the first stable statement of the fact.
2. Keep one table or diagram when it materially lowers the cost of understanding.
3. In later sections, expand only new mechanisms, conditions, or failure paths.
4. In the summary, return to the public contract, core dataflow, and current boundaries without repeating experiment tables.

Do not repeat role definitions merely to make every section appear self-contained. Once a role is defined in the overview, later diagram introductions should explain only meanings specific to that diagram.

### 3.6 Sweep five: pre-publication recheck

After a pass, work backward and confirm that later edits did not invalidate earlier decisions:

- Terms, section numbers, and cross-references remain consistent.
- Tables, code fences, disclosure blocks, and Mermaid diagrams parse correctly.
- Chinese and English pages express the same contracts and boundaries.
- Exact identifiers still use their real names.
- The edit did not introduce a compatibility path, configuration entry, or unverified fact.
- The documentation site builds successfully.

## 4. Collaborative review and one-shot example

### 4.1 Trace surface issues to root causes

An editing comment should identify the location, problem, reason, and proposed change. “This reads awkwardly” alone is not enough to justify an edit.

| Item | Example |
| --- | --- |
| Location | The experiment paragraph in the opening. |
| Problem | It introduces measurement details before the reader understands the document structure. |
| Reason | Local evidence occupies the introduction's abstraction level. |
| Proposed change | Keep only the document map in the opening and move measurement conditions to the experiment section. |

For clear, local issues that do not change technical meaning, edit directly and recheck the result. Before changing a contract, claim scope, or section narrative, explain the evidence and impact.

A complete pass should also identify the common causes behind multiple surface issues. This lets the editor correct the full document and reuse the lesson on the next one. The following table uses a set of actual edits to the KV SSD article:

| Surface issue | Root cause | Editing decision |
| --- | --- | --- |
| “First, remember four stable conclusions” introduces a summary. | The label is stronger than the content; a summary is not yet a supported conclusion. | Use “four core points.” |
| The prose states the owner's physical resources, switches to the master, and then returns to the owner's SSD scheduling. | One subject's responsibilities are fragmented, forcing readers to reconstruct the role boundary. | Group by subject: explain the master completely, then the owner. |
| The text only says that the master schedules memory allocations. | The responsibility claim lacks a nearby rationale, so readers cannot tell whether the boundary is complete. | Immediately state that memory allocations cover both final value replicas and temporary transfer memory, and that both interact with `route` and in-flight state. |
| The opening explains Section 9 experiment groups, hit conditions, and measurement methodology. | Local evidence and the introduction occupy different abstraction levels, hiding the document map. | Keep only the problem, core points, and reading path in the opening; leave experiment conditions in Section 9. |
| “Cover a larger runtime working set” is presented as an already realized result. | A design expectation is stated as a verified result, making the claim stronger than the evidence and leaving the reader-facing runtime benefit implicit. | Preserve the unchanged-public-API scope and say that the design is expected to raise cache hit rates and make fuller use of storage bandwidth. |

These edits serve two shared goals: help readers build the system model at the right level and keep every claim within its evidence boundary.

### 4.2 Reusable one-shot example

Use the following prompt after the technical facts have been verified and the document needs a complete structural and language review:

```text
Target document: <document path>

Perform one complete copy-editing pass on the target technical document and edit it directly. Preserve public APIs, implementation facts, benchmark numbers, and existing claim boundaries.

Check the following:
1. The opening states only the problem, core points, and document map. It does not preload local implementation detail or experiment methodology.
2. Labels in headings, introductions, and table headers match their content. Do not call summaries or capability states “conclusions.”
3. Group responsibilities by subject. When introducing multiple roles, finish one role before moving to the next.
4. Place the reason or mechanism next to every important architecture claim, especially responsibility boundaries, lifetimes, and data direction.
5. Match claim strength to evidence. State verified results directly; qualify design goals or expected benefits with “expected,” “aims to,” or “may”; do not present unsupported outcomes as facts.
6. Remove repeated role descriptions, details at the wrong abstraction level, and summaries that add no information. Preserve necessary scope, preconditions, and exclusions.
7. Keep terminology, type names, API names, section numbers, cross-references, tables, and code fences consistent.

After editing:
- Summarize the main changes as “surface issue → root cause → editing decision” instead of listing every sentence edit.
- Update the corresponding Chinese or English page when one exists.
- Recheck public contracts, technical facts, and performance methodology, then run the repository's required documentation-site build.
```

## 5. Upstream practices not adopted here

- Do not use emotional amplification, FOMO, exaggerated pain, or sales language in technical articles.
- Do not force a user benefit onto every implementation fact. Explain consequences only when they clarify rationale or trade-offs.
- Do not add unsourced numbers, time commitments, case studies, or comparative claims.
- Do not mechanically convert English guidance such as “no more than 25 words per sentence” into Chinese character limits.
- Do not force every sentence into active voice; ownership and dataflow clarity take priority.
- Do not use scores from invented expert personas as a substitute for code, tests, or same-level end-to-end tracing.

## 6. Updating the upstream snapshot

This document is pinned to upstream commit `0ba2a7f`. To update it:

1. Compare upstream `skills/copy-editing/SKILL.md` and `references/` with the pinned revision.
2. Import only changes that apply to technical documentation and do not conflict with Fluxon rules.
3. Update the Chinese and English documents, skill version, and pinned commit together.
4. Rebuild the documentation site.

Do not overwrite this document automatically with upstream content. The upstream target is marketing copy; this document targets accurate, verifiable technical writing.

## 7. Third-party license

This document is based on Corey Haines's `copy-editing` skill and has been modified for Fluxon technical documentation. The upstream work is licensed under the MIT License.

<details>
<summary>MIT License</summary>

```text
MIT License

Copyright (c) 2025 Corey Haines

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```

</details>
