# Accepted-write recovery journal

The privileged installer must run after reviewed migrations, with every writer drained,
and commit before any application or control writer resumes. Supply every canonical
and active control schema. Installation locks registered base tables atomically;
changed table definitions fail closed on reinstall. Journal identifiers are allocation
order, never commit watermarks. Retain transaction IDs and the complete immutable,
private journal, including SQL-null metadata.

Application, worker and control login roles must not own captured tables or schemas,
inherit their owner/migrator role, have CREATE on captured schemas, or be superusers.
Grant only the required data privileges. Check these permissions separately when
wiring the runtime; the installer does not create or certify application roles.
PostgreSQL reserves ALTER/DROP for owners or privileged roles, so these restrictions
prevent ordinary writers changing the journal's source definitions.

Only a reviewed privileged maintenance process may change or drop captured tables.
It must pause and drain all writers first, preserve the prior schema/database/journal,
apply the reviewed migration, and establish a reviewed fresh journal configuration
before resuming writes. An installer signature mismatch is a stop condition. Do not
resume ordinary writes after privileged DDL merely because existing triggers remain;
static SQL-null metadata and archived source types may no longer match the new
schema. The installer is a DML capture mechanism, not a DDL guard or complete disaster
recovery system. A signature mismatch does not authorize deleting its configuration
or its retained journal.

The recovery design supports a compatible application/worker restart retaining
the current canonical database; verify it against the exact image, schema and
client workload in a disposable rehearsal before operational use. Preserve accepted operations, idempotency keys,
corrections, evidence, audit history and lifecycle state, and verify them through the
compatible authenticated clients after restart. The journal rejects writes with
asynchronous commits. This does not prove an old schema/application can recover
new writes. Across generations, do not restore an earlier database or restart old
independent authoritative writers without a separately proven zero-loss replay.
Exact-image rollback, complete database disaster recovery, and every legacy client's
accepted-write recovery each require their own evidence.
