# file-manager

A set of traits that abstract file interactions to enable better testability and
flexibility.

This crate allows Rust developers to write code that typically operates on
files, but can instead operate against another data structure. For example, this
crate offers two implementations:

- `StdFileManager`: A `FileManager` implementation powered by
  `std::fs`.
- `MemoryFileManager`: A `FileManager` implementation that is powered fully by
  in-memory structures.

This common abstraction layer is being adopted into [OkayWAL][okaywal],
[Sediment][sediment], [Nebari][nebari], and eventually [BonsaiDb][bonsaidb],
allowing the entire stack to support both file-based and in-memory databases.

## Future goals

- Ability to simulate IO errors with MemoryFileManager
  - Maximum space limit to simulate disk full errors
  - Bad regions: allow blocking reads and/or writes to specific regions of
    files.
- Add support for `fcntl(F_FBARRIERFSYNC)`.
- Unify directory syncing logic between [Sediment][sediment] and
  [OkayWAL][okaywal].

[okaywal]: https://github.com/khonsulabs/okaywal
[sediment]: https://github.com/khonsulabs/sediment
[nebari]: https://github.com/khonsulabs/nebari
[bonsaidb]: https://github.com/khonsulabs/bonsaidb
