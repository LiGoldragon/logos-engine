# logos-engine architecture
A real daemon and thin CLI. Signal → Nexus → SEMA Kameo actors fetch CoreLogos from central Sema and project it through the real TextualRust codec. The producer closure now uses one full exact CoreLogos revision, so projection consumes the stored typed item directly.

## The generated-module head

A projected module opens with the fixed head — the `// @generated` marker, the four scalar type aliases, and the cfg-gated NOTA import — which is a property of the module shape, not of any schema. That head is Nomos's fixed prelude package (`core_nomos::ModuleHead`); the engine prepends `ModuleHead::render()` ahead of the document's declaration items. The prelude is rendered inside `core-nomos` through the same prettyplease projection and handed here as a plain `String`, so no typed CoreLogos value crosses the boundary.

That String seam is deliberate (a recorded lean): `core-nomos`'s prelude items are the new CoreLogos revision (they use the `Use` item kind and the `Cfg` attribute), while the stored logos documents this engine decodes are the CoreLogos revision `signal-sema-storage` pins. Both revisions coexist in the lock; because the prelude crosses only as text, the declaration-decode path stays on the storage revision and needs no lockstep bump of the storage chain. When `signal-sema-storage` next advances to the new CoreLogos revision, the two versions collapse back to one.
