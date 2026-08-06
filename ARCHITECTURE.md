# logos-engine architecture

`logos-engine` is a real daemon and thin CLI. In the currently wired legacy topology, its Signal → Nexus → SEMA actor path fetches a Logos document through the central socket. The approved target remains a stateful Logos daemon whose own embedded sema database stores its state; the separate translator daemon owns only shared naming and encoded-ID allocation. This migration does not extend or present the central store as the target architecture.

## Stored truth and projection

The live storage arm is no longer a list of legacy items. It carries two private canonical archives:

- `WholeLogosArchive`, restored only through `WholeLogos::from_archive_bytes`, including encoded-ID and nonempty-product validation;
- `VocabularyNameTableArchive`, restored only through the production nested name-table archive boundary.

The engine crosses both checked boundaries before projection. It never reconstructs `WholeLogos` from legacy `EncodedItem` values, reads flat name-table bytes, or translates between identity schemes.

Rust emission is owned by current `rust-logos::RustLogos`. The stored authority table supplies the exact Rust-root allocations for the five structural record identities and the five fixed Rust tokens required to seal the emitter. Missing allocations are refused; the engine does not mint them or substitute spellings. Production emission derives Universal declaration tokens from their complete encoded-ID chains and resolves immutable Rust-root references through the restored authority table. Thus an operational rename cannot move a Universal emitted identity, while a Rust vocabulary reference remains authority-resolved.

## Generated-module head

Every generated module still opens with Nomos's fixed module head: the generated marker, scalar aliases, and cfg-gated NOTA import. `core_nomos::ModuleHead` owns and renders that fixed package, and the engine prepends the returned string to the one complete `RustLogos::emit` result. This string seam is output assembly, not an archive or version bridge; the stored body remains a single typed `WholeLogos` value throughout.

## Retired legacy Nomos relay

The former CLI accepted a Nomos socket and drove projection from `signal-nomos::Subscribe/Event`. That operation pair no longer exists in the current stateful Nomos contract. Its replacement `Transform` returns a checked but opaque native `LogosPopulationArchive`, not a central-storage `ContentHash`; treating those bytes as a stored Logos document would invent archive meaning. The relay and filesystem socket watcher are therefore removed, and the typed CLI daemon surface is now `daemon [socket] [sema-socket]`. Direct `ProjectRust`, `List`, and Logos `Subscribe` behavior remains. Automatic cross-daemon projection can return only through a separately designed stateful event/archive boundary.

## Exact train

The normal graph pins the published current owners it actually uses: `signal-sema-storage` 0.3.0, `signal-logos` 0.2.0, `core-nomos` 0.39.0, `core-logos` 0.25.0, and `rust-logos` 0.29.0. The current storage, whole-Logos, nested naming, translator, and Rust projection owners each resolve to one revision. `signal-nomos` is deliberately absent because this engine implements no current Nomos operation; keeping an unused compatibility pin would claim ownership the engine does not have. Older name-table/content-identity crates remain only where the current producers deliberately retain their legacy internal surfaces; no obsolete aggregate-Protos or core-schema archive owner participates in the engine's live Logos path.

This is the archive-truth migration required by `primary-e68.4`. It does not begin the separately staged identity car: no content identity is minted, inferred, rebound, or converted here.
