# Known non-idealities

- **No-alias producer transition:** `core-nomos` and `textual-rust` now consume the encoded Logos producer family, while `signal-sema-storage` still emits its older `DocumentPayload::Logos` item. `logos-engine` cannot project that stored item until the signal/SEMA contract train advances to the same producer revisions. Do not add a conversion, compatibility alias, or second projection path here. The proper fix is the ordered signal/daemon consumer re-pin cascade, followed by the language-engine witness.
