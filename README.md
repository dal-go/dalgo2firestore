# [strongo/dalgo-firestore](https://github.com/dal-go/dalgo2firestore)

Bridge to Firebase Firestore API for [`github.com/dal-go/dalgo`](https://github.com/dal-go/dalgo) interface.

<!-- dev-approach:v1 -->
## Our approach to development

We build with our own tooling:

- **[SpecScore](https://specscore.md)** — specify requirements as `SpecScore.md` artifacts
- **[SpecStudio](https://specscore.studio)** — author & manage specs across their lifecycle
- **[inGitDB](https://ingitdb.com)** — store structured data in Git where applicable
- **[DALgo](https://dalgo.io)** — data access layer for Go
- **[cover100.dev](https://cover100.dev)** — drive toward 100% test coverage
- **[DataTug](https://datatug.io)** — query & explore data
<!-- /dev-approach -->

## Transaction queries

Query readers created by a read-only or read-write transaction use that exact
Firestore transaction, including its snapshot and read-after-write protection.
Use the callback's context and consume and close readers before the transaction
callback returns. Closing a reader stops its Firestore document iterator.
Ordinary database queries retain their nontransactional behavior.

## [End-to-end tests](end2end)

All `dalgo` drivers have to pass [dalgo's end-to-end tests](https://github.com/dal-go/dalgo/end2end).

Setup of [end-to-end tests for Firestore driver](end2end/README.md) has few specifics. 