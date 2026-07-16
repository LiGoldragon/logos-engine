# Non-idealities
- Symptom: Cargo treats `core-logos?rev=17cbd7596df2` and the same full commit revision as distinct crate identities. Current workaround: validated rkyv round trip at the contact point. Proper fix: normalize `core-nomos` and `textual-rust` manifests to the same full pushed revision in a producer-owned release slice.
