# Agent guidance

The actor-plane flow and central typed reads are wired legacy behavior, not
the target architecture. The approved target makes this daemon stateful, with
its own embedded sema database. A separate small translator daemon is
authoritative only for shared naming and encodedID allocation.

Do not extend the stateless-client pattern or imply that the storage migration
has landed. Preserve TextualRust as the currently wired projection authority.

This repository is under fast development and constantly breaking.
