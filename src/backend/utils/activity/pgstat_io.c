/* -------------------------------------------------------------------------
 *
 * pgstat_io.c
 *	  Implementation of IO statistics.
 *
 * This file contains the implementation of IO statistics. It is kept separate
 * from pgstat.c to enforce the line between the statistics access / storage
 * implementation and the details about individual types of statistics.
 *
 * Copyright (c) 2021-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_io.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/pgstat_internal.h"


static PgStat_IOContextOps pending_IOOpStats;
bool		have_iostats = false;


void
pgstat_count_io_op(IOOp io_op, IOObject io_object, IOContext io_context)
{
	Assert(io_context < IOCONTEXT_NUM_TYPES);
	Assert(io_object < IOOBJECT_NUM_TYPES);
	Assert(io_op < IOOP_NUM_TYPES);
	Assert(pgstat_tracks_io_op(MyBackendType, io_context, io_object, io_op));

	pending_IOOpStats.data[io_context].data[io_object].data[io_op]++;

	have_iostats = true;
}

PgStat_IO *
pgstat_fetch_stat_io(void)
{
	pgstat_snapshot_fixed(PGSTAT_KIND_IO);

	return &pgStatLocal.snapshot.io;
}

/*
 * Flush out locally pending IO statistics
 *
 * If no stats have been recorded, this function returns false.
 *
 * If nowait is true, this function returns true if the lock could not be
 * acquired. Otherwise, return false.
 */
bool
pgstat_flush_io(bool nowait)
{
	LWLock	   *bktype_lock;
	PgStat_IOContextOps *bktype_shstats;

	if (!have_iostats)
		return false;

	bktype_lock = &pgStatLocal.shmem->io.locks[MyBackendType];
	bktype_shstats =
		&pgStatLocal.shmem->io.stats[MyBackendType];

	if (!nowait)
		LWLockAcquire(bktype_lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(bktype_lock, LW_EXCLUSIVE))
		return true;

	for (IOContext io_context = IOCONTEXT_BULKREAD;
		 io_context < IOCONTEXT_NUM_TYPES; io_context++)
		for (IOObject io_object = IOOBJECT_RELATION;
			 io_object < IOOBJECT_NUM_TYPES; io_object++)
			for (IOOp io_op = IOOP_EVICT;
				 io_op < IOOP_NUM_TYPES; io_op++)
				bktype_shstats->data[io_context].data[io_object].data[io_op] +=
					pending_IOOpStats.data[io_context].data[io_object].data[io_op];

	Assert(pgstat_bktype_io_stats_valid(bktype_shstats, MyBackendType));

	LWLockRelease(bktype_lock);

	memset(&pending_IOOpStats, 0, sizeof(pending_IOOpStats));

	have_iostats = false;

	return false;
}

const char *
pgstat_get_io_context_name(IOContext io_context)
{
	switch (io_context)
	{
		case IOCONTEXT_BULKREAD:
			return "bulkread";
		case IOCONTEXT_BULKWRITE:
			return "bulkwrite";
		case IOCONTEXT_NORMAL:
			return "normal";
		case IOCONTEXT_VACUUM:
			return "vacuum";
	}

	elog(ERROR, "unrecognized IOContext value: %d", io_context);
	pg_unreachable();
}

const char *
pgstat_get_io_object_name(IOObject io_object)
{
	switch (io_object)
	{
		case IOOBJECT_RELATION:
			return "relation";
		case IOOBJECT_TEMP_RELATION:
			return "temp relation";
	}

	elog(ERROR, "unrecognized IOObject value: %d", io_object);
	pg_unreachable();
}

const char *
pgstat_get_io_op_name(IOOp io_op)
{
	switch (io_op)
	{
		case IOOP_EVICT:
			return "evicted";
		case IOOP_EXTEND:
			return "extended";
		case IOOP_FSYNC:
			return "files synced";
		case IOOP_READ:
			return "read";
		case IOOP_REUSE:
			return "reused";
		case IOOP_WRITE:
			return "written";
	}

	elog(ERROR, "unrecognized IOOp value: %d", io_op);
	pg_unreachable();
}

void
pgstat_io_reset_all_cb(TimestampTz ts)
{
	for (int i = 0; i < BACKEND_NUM_TYPES; i++)
	{
		LWLock	   *bktype_lock = &pgStatLocal.shmem->io.locks[i];
		PgStat_IOContextOps *bktype_shstats = &pgStatLocal.shmem->io.stats[i];

		LWLockAcquire(bktype_lock, LW_EXCLUSIVE);

		/*
		 * Use the lock in the first BackendType's PgStat_IOContextOps to
		 * protect the reset timestamp as well.
		 */
		if (i == 0)
			pgStatLocal.shmem->io.stat_reset_timestamp = ts;

		memset(bktype_shstats, 0, sizeof(*bktype_shstats));
		LWLockRelease(bktype_lock);
	}
}

void
pgstat_io_snapshot_cb(void)
{
	for (int i = 0; i < BACKEND_NUM_TYPES; i++)
	{
		LWLock	   *bktype_lock = &pgStatLocal.shmem->io.locks[i];
		PgStat_IOContextOps *bktype_shstats = &pgStatLocal.shmem->io.stats[i];
		PgStat_IOContextOps *bktype_snap = &pgStatLocal.snapshot.io.stats[i];

		LWLockAcquire(bktype_lock, LW_SHARED);

		/*
		 * Use the lock in the first BackendType's PgStat_IOContextOps to
		 * protect the reset timestamp as well.
		 */
		if (i == 0)
			pgStatLocal.snapshot.io.stat_reset_timestamp =
				pgStatLocal.shmem->io.stat_reset_timestamp;

		/* using struct assignment due to better type safety */
		*bktype_snap = *bktype_shstats;
		LWLockRelease(bktype_lock);
	}
}

/*
* IO statistics are not collected for all BackendTypes.
*
* The following BackendTypes do not participate in the cumulative stats
* subsystem or do not perform IO on which we currently track:
* - Syslogger because it is not connected to shared memory
* - Archiver because most relevant archiving IO is delegated to a
*   specialized command or module
* - WAL Receiver and WAL Writer IO is not tracked in pg_stat_io for now
*
* Function returns true if BackendType participates in the cumulative stats
* subsystem for IO and false if it does not.
*/
bool
pgstat_tracks_io_bktype(BackendType bktype)
{
	/*
	 * List every type so that new backend types trigger a warning about
	 * needing to adjust this switch.
	 */
	switch (bktype)
	{
		case B_INVALID:
		case B_ARCHIVER:
		case B_LOGGER:
		case B_WAL_RECEIVER:
		case B_WAL_WRITER:
			return false;

		case B_AUTOVAC_LAUNCHER:
		case B_AUTOVAC_WORKER:
		case B_BACKEND:
		case B_BG_WORKER:
		case B_BG_WRITER:
		case B_CHECKPOINTER:
		case B_STANDALONE_BACKEND:
		case B_STARTUP:
		case B_WAL_SENDER:
			return true;
	}

	return false;
}

/*
 * Some BackendTypes do not perform IO in certain IOContexts. Some IOObjects
 * are never operated on in some IOContexts. Check that the given BackendType
 * is expected to do IO in the given IOContext and that the given IOObject is
 * expected to be operated on in the given IOContext.
 */
bool
pgstat_tracks_io_object(BackendType bktype, IOContext io_context,
						IOObject io_object)
{
	bool		no_temp_rel;

	/*
	 * Some BackendTypes should never track IO statistics.
	 */
	if (!pgstat_tracks_io_bktype(bktype))
		return false;

	/*
	 * Currently, IO on temporary relations can only occur in the
	 * IOCONTEXT_NORMAL IOContext.
	 */
	if (io_context != IOCONTEXT_NORMAL &&
		io_object == IOOBJECT_TEMP_RELATION)
		return false;

	/*
	 * In core Postgres, only regular backends and WAL Sender processes
	 * executing queries will use local buffers and operate on temporary
	 * relations. Parallel workers will not use local buffers (see
	 * InitLocalBuffers()); however, extensions leveraging background workers
	 * have no such limitation, so track IO on IOOBJECT_TEMP_RELATION for
	 * BackendType B_BG_WORKER.
	 */
	no_temp_rel = bktype == B_AUTOVAC_LAUNCHER || bktype == B_BG_WRITER ||
		bktype == B_CHECKPOINTER || bktype == B_AUTOVAC_WORKER ||
		bktype == B_STANDALONE_BACKEND || bktype == B_STARTUP;

	if (no_temp_rel && io_context == IOCONTEXT_NORMAL &&
		io_object == IOOBJECT_TEMP_RELATION)
		return false;

	/*
	 * Some BackendTypes do not currently perform any IO in certain
	 * IOContexts, and, while it may not be inherently incorrect for them to
	 * do so, excluding those rows from the view makes the view easier to use.
	 */
	if ((bktype == B_CHECKPOINTER || bktype == B_BG_WRITER) &&
		(io_context == IOCONTEXT_BULKREAD ||
		 io_context == IOCONTEXT_BULKWRITE ||
		 io_context == IOCONTEXT_VACUUM))
		return false;

	if (bktype == B_AUTOVAC_LAUNCHER && io_context == IOCONTEXT_VACUUM)
		return false;

	if ((bktype == B_AUTOVAC_WORKER || bktype == B_AUTOVAC_LAUNCHER) &&
		io_context == IOCONTEXT_BULKWRITE)
		return false;

	return true;
}

/*
 * Some BackendTypes will never do certain IOOps and some IOOps should not
 * occur in certain IOContexts. Check that the given IOOp is valid for the
 * given BackendType in the given IOContext. Note that there are currently no
 * cases of an IOOp being invalid for a particular BackendType only within a
 * certain IOContext.
 */
bool
pgstat_tracks_io_op(BackendType bktype, IOContext io_context,
					IOObject io_object, IOOp io_op)
{
	bool		strategy_io_context;

	/* if (io_context, io_object) will never collect stats, we're done */
	if (!pgstat_tracks_io_object(bktype, io_context, io_object))
		return false;

	/*
	 * Some BackendTypes will not do certain IOOps.
	 */
	if ((bktype == B_BG_WRITER || bktype == B_CHECKPOINTER) &&
		(io_op == IOOP_READ || io_op == IOOP_EVICT))
		return false;

	if ((bktype == B_AUTOVAC_LAUNCHER || bktype == B_BG_WRITER ||
		 bktype == B_CHECKPOINTER) && io_op == IOOP_EXTEND)
		return false;

	/*
	 * Some IOOps are not valid in certain IOContexts and some IOOps are only
	 * valid in certain contexts.
	 */
	if (io_context == IOCONTEXT_BULKREAD && io_op == IOOP_EXTEND)
		return false;

	strategy_io_context = io_context == IOCONTEXT_BULKREAD ||
		io_context == IOCONTEXT_BULKWRITE || io_context == IOCONTEXT_VACUUM;

	/*
	 * IOOP_REUSE is only relevant when a BufferAccessStrategy is in use.
	 */
	if (!strategy_io_context && io_op == IOOP_REUSE)
		return false;

	/*
	 * IOOP_FSYNC IOOps done by a backend using a BufferAccessStrategy are
	 * counted in the IOCONTEXT_NORMAL IOContext. See comment in
	 * ForwardSyncRequest() for more details.
	 */
	if (strategy_io_context && io_op == IOOP_FSYNC)
		return false;

	/*
	 * Temporary tables are not logged and thus do not require fsync'ing.
	 */
	if (io_context == IOCONTEXT_NORMAL &&
		io_object == IOOBJECT_TEMP_RELATION && io_op == IOOP_FSYNC)
		return false;

	return true;
}

/*
 * Check that stats have not been counted for any combination of IOContext,
 * IOObject, and IOOp which are not tracked for the passed-in BackendType. The
 * passed-in array of PgStat_IOOps must contain stats from the
 * BackendType specified by the second parameter. Caller is responsible for
 * locking of the passed-in PgStat_IOContextOps, if needed.
 */
bool
pgstat_bktype_io_stats_valid(PgStat_IOContextOps *context_ops,
							 BackendType bktype)
{
	bool		bktype_tracked = pgstat_tracks_io_bktype(bktype);

	for (IOContext io_context = IOCONTEXT_BULKREAD;
		 io_context < IOCONTEXT_NUM_TYPES; io_context++)
	{
		for (IOObject io_object = IOOBJECT_RELATION;
			 io_object < IOOBJECT_NUM_TYPES; io_object++)
		{
			for (IOOp io_op = IOOP_EVICT; io_op < IOOP_NUM_TYPES; io_op++)
			{
				if ((!bktype_tracked || !pgstat_tracks_io_op(bktype, io_context, io_object, io_op)) &&
					 context_ops->data[io_context].data[io_object].data[io_op] != 0)
					return false;
			}
		}
	}

	return true;
}
