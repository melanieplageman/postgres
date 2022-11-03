/* -------------------------------------------------------------------------
 *
 * pgstat_io_ops.c
 *	  Implementation of IO operation statistics.
 *
 * This file contains the implementation of IO operation statistics. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2021-2022, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_io_ops.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "utils/pgstat_internal.h"

static PgStat_IOContextOps pending_IOOpStats;

void
pgstat_count_io_op(IOOp io_op, IOObject io_object, IOContext io_context)
{
	PgStat_IOOpCounters *pending_counters;

	Assert(io_context < IOCONTEXT_NUM_TYPES);
	Assert(io_op < IOOP_NUM_TYPES);
	Assert(pgstat_expect_io_op(MyBackendType, io_context, io_object, io_op));

	pending_counters = &pending_IOOpStats.data[io_context].data[io_object];

	switch (io_op)
	{
		case IOOP_EVICT:
			pending_counters->evictions++;
			break;
		case IOOP_EXTEND:
			pending_counters->extends++;
			break;
		case IOOP_FSYNC:
			pending_counters->fsyncs++;
			break;
		case IOOP_READ:
			pending_counters->reads++;
			break;
		case IOOP_REUSE:
			pending_counters->reuses++;
			break;
		case IOOP_WRITE:
			pending_counters->writes++;
			break;
	}

}

const char *
pgstat_io_context_desc(IOContext io_context)
{
	switch (io_context)
	{
		case IOCONTEXT_BULKREAD:
			return "bulkread";
		case IOCONTEXT_BULKWRITE:
			return "bulkwrite";
		case IOCONTEXT_BUFFER_POOL:
			return "buffer pool";
		case IOCONTEXT_VACUUM:
			return "vacuum";
	}

	elog(ERROR, "unrecognized IOContext value: %d", io_context);
}

const char *
pgstat_io_object_desc(IOObject io_object)
{
	switch(io_object)
	{
		case IOOBJECT_RELATION:
			return "relation";
		case IOOBJECT_TEMP_RELATION:
			return "temp relation";
	}

	elog(ERROR, "unrecognized IOObject value: %d", io_object);
}

const char *
pgstat_io_op_desc(IOOp io_op)
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
}

/*
* IO Operation statistics are not collected for all BackendTypes.
*
* The following BackendTypes do not participate in the cumulative stats
* subsystem or do not do IO operations worth reporting statistics on:
* - Syslogger because it is not connected to shared memory
* - Archiver because most relevant archiving IO is delegated to a
*   specialized command or module
* - WAL Receiver and WAL Writer IO is not tracked in pg_stat_io for now
*
* Function returns true if BackendType participates in the cumulative stats
* subsystem for IO Operations and false if it does not.
*/
bool
pgstat_io_op_stats_collected(BackendType bktype)
{
	return bktype != B_INVALID && bktype != B_ARCHIVER && bktype != B_LOGGER &&
		bktype != B_WAL_RECEIVER && bktype != B_WAL_WRITER;
}


/*
 * Some BackendTypes do not perform IO operations in certain IOContexts. Some
 * IOObjects are never operated on in some IOContexts. Check that the given
 * BackendType is expected to do IO in the given IOContext and that the given
 * IOObject is expected to be operated on in the given IOContext..
 */
bool
pgstat_bktype_io_context_io_object_valid(BackendType bktype,
		IOContext io_context, IOObject io_object)
{
	bool		no_temp_rel;

	/*
	 * Currently, IO operations on temporary relations can only occur in the
	 * IOCONTEXT_BUFFER_POOL IOContext.
	 */
	if (io_context != IOCONTEXT_BUFFER_POOL &&
			io_object == IOOBJECT_TEMP_RELATION)
		return false;

	/*
	 * In core Postgres, only regular backends and WAL Sender processes
	 * executing queries will use local buffers and operate on temporary
	 * relations. Parallel workers will not use local buffers (see
	 * InitLocalBuffers()); however, extensions leveraging background workers
	 * have no such limitation, so track IO Operations on
	 * IOOBJECT_TEMP_RELATION for BackendType B_BG_WORKER.
	 */
	no_temp_rel = bktype == B_AUTOVAC_LAUNCHER || bktype == B_BG_WRITER || bktype
		== B_CHECKPOINTER || bktype == B_AUTOVAC_WORKER || bktype ==
		B_STANDALONE_BACKEND || bktype == B_STARTUP;

	if (no_temp_rel && io_context == IOCONTEXT_BUFFER_POOL && io_object ==
			IOOBJECT_TEMP_RELATION)
		return false;

	/*
	 * Some BackendTypes do not currently perform any IO operations in certain
	 * IOContexts, and, while it may not be inherently incorrect for them to
	 * do so, excluding those rows from the view makes the view easier to use.
	 */
	if ((bktype == B_CHECKPOINTER || bktype == B_BG_WRITER) &&
			(io_context == IOCONTEXT_BULKREAD || io_context ==
			 IOCONTEXT_BULKWRITE || io_context == IOCONTEXT_VACUUM))
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
pgstat_io_op_valid(BackendType bktype, IOContext io_context, IOObject io_object, IOOp io_op)
{
	bool		strategy_io_context;

	/*
	 * Some BackendTypes should never track IO Operation statistics.
	 */
	Assert(pgstat_io_op_stats_collected(bktype));

	/*
	 * Some BackendTypes will not do certain IOOps.
	 */
	if ((bktype == B_BG_WRITER || bktype == B_CHECKPOINTER) &&
		(io_op == IOOP_READ || io_op == IOOP_EVICT))
		return false;

	if ((bktype == B_AUTOVAC_LAUNCHER || bktype == B_BG_WRITER || bktype ==
		 B_CHECKPOINTER) && io_op == IOOP_EXTEND)
		return false;

	/*
	 * Some IOOps are not valid in certain IOContexts and some IOOps are only
	 * valid in certain contexts.
	 */
	if (io_context == IOCONTEXT_BULKREAD && io_op == IOOP_EXTEND)
		return false;

	strategy_io_context = io_context == IOCONTEXT_BULKREAD || io_context ==
		IOCONTEXT_BULKWRITE || io_context == IOCONTEXT_VACUUM;

	/*
	 * IOOP_REUSE is only relevant when a BufferAccessStrategy is in use.
	 */
	if (!strategy_io_context && io_op == IOOP_REUSE)
		return false;

	 /*
	 * IOOP_FSYNC IOOps done by a backend using a BufferAccessStrategy are
	 * counted in the IOCONTEXT_BUFFER_POOL IOContext. See comment in
	 * ForwardSyncRequest() for more details.
	 */
	if (strategy_io_context  && io_op == IOOP_FSYNC)
		return false;

	/*
	 * Temporary tables are not logged and thus do not require fsync'ing.
	 */
	if (io_context == IOCONTEXT_BUFFER_POOL && io_object ==
			IOOBJECT_TEMP_RELATION && io_op == IOOP_FSYNC)
		return false;

	return true;
}

bool
pgstat_expect_io_op(BackendType bktype, IOContext io_context, IOObject io_object, IOOp io_op)
{
	if (!pgstat_io_op_stats_collected(bktype))
		return false;

	if (!pgstat_bktype_io_context_io_object_valid(bktype, io_context, io_object))
		return false;

	if (!(pgstat_io_op_valid(bktype, io_context, io_object, io_op)))
		return false;

	return true;
}
