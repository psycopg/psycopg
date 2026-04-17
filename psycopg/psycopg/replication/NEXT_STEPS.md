[X] Use server_encoding for decoding, where appropriate
[X] Fix handling of relations in streaming transactions
- Analyze FIXMEs and update this document
- Locking the cursor????
- Implement physical replication with alternate timeline handling
[X] Handle RowValue.UNCHANGED in Transformer
[X] Add slots
- Add more cython implementations (should be able to do most of the decoding work in C)
- Option to decode differently at a higher level (e.g. not wrap everything in XLogData)
- Implement BASE_BACKUP for PG 14???
[X] Support Two-phase messages
[X] Replace template strings with SQL processing
- Should we use the server timezone for our timestamps?
[X] add pq tracing to tests
- Pass additional relation information to RowFactories
  This could be used in dispatching to appropriate classes.
[X] Allow decoders to skip messages by returning None
- Allow decoders to return multiple messages by returning a sequence
  (maybe a specific type of sequence, generator?, so it doesn't conflict)
- Fix two-phase tests and maybe messages  (some tests sometimes fail.
  I think I've interpretted end_lsn wrong).


- Missing Tests:
    - non-ascii type names and relation names
    - Add row-factory tests for base_backup and other admin commands
    - Test physical replication with alternate timelines.
        - and implement
    - Test consume_* methods
    - Concurrent access tests / thread safety
    - Test decoder message skipping by returning None
    [X] test ORIGIN messages
    [X] test TYPE messages
    [X] test multiple publications
    [X] test cancelling future in read_* doesn't cancel the main query (BASE_BACKUP or START_REPLICATION)
    [X] Test client_encoding with Physical connection
    [X] Test different logical row_factories
    [X] Test changing the adapt loaders mid-stream
    [X] Test remaining PgOutput Message types
    [X] Test Streaming transactions
- More Tests:
    [X] use replication cursors with regular connections
    - use replication connections with regular cursors
    [X] BaseReplicationCursor methods parametrized on the three cursor types
    [X] Fix or remove the decoding unit tests
    [X] Basic tests for logical decoding with decoder=None
    [X] pgoutput: test streaming abort subtransaction
    - test origin with two-phase
    - typing tests?


- Docs:
    [X] High-level summary
    [X] API
    [X] Decoders
    [X] Logical Row Factories


- Future Ideas:
    [X] inline relations (shouldn't have an XID, should support inline types?)
    - inline types
    - Physical decoder similar to pg_wal_dump
    - LazyPgOutputDecoder?
    - wal2json output plugin (or maybe just a general-purpose JsonDecoder)
