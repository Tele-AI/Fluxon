This directory vendors `offset-allocator` 0.2.0 from crates.io, upstream commit
`9449d4d81be91396938b05894fad443f63093077`. `UPSTREAM.json` records the original
file hashes. The upstream MIT license remains in `LICENSE-MIT`.

The Fluxon patch adds `storage_report_for_size(size)` to compute exact physical
free-region geometry from the existing free-node chains, and `layout_revision()`
to invalidate owner-side report caches after successful allocate, free, or reset.
It preserves the allocator's placement, splitting, merging, and admission rules.
Revision overflow is rejected before mutation; versions are local to an allocator
instance and must be paired with its identity when used as cache keys.

The exact report uses actual node lengths rather than rounded bin sizes. It
describes physical capacity; exhausted metadata or conservative bin selection
can still make a real allocation fail. A zero accounting size is rejected.
`src/exact_tests.rs` covers fragmentation, coarse bins, metadata exhaustion,
revision invalidation and overflow, and randomized occupancy-bitmap comparison.
