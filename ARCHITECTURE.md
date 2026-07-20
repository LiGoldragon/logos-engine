# logos-engine architecture

A real daemon and thin CLI. Signal → Nexus → SEMA Kameo actors fetch encoded Logos documents from central SEMA and project them through TextualRust.

## The generated-module head

A projected module opens with Nomos's fixed head. The head supplies the generated marker and canonical support imports, then this engine appends the document's projected declaration items. It contains no transparent type aliases. Nomos renders the head through the same TextualRust projection and hands it to this engine as output text; no encoded Logos item crosses that assembly boundary.

`DocumentPayload::Logos` and `RustSource::project_item` must always use the same encoded Logos revision. Advancing Nomos or TextualRust before the SEMA-storage contract advances produces distinct Rust types for the same nominal document item, which is rejected at compilation rather than bridged through conversion or a compatibility alias. The required consumer train advances that contract before this engine becomes buildable against the new producer family.
