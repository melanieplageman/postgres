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
pgstat_count_io_op(IOOp io_op, IOContext io_context)
{
	PgStat_IOOpCounters *pending_counters;

	Assert(io_context < IOCONTEXT_NUM_TYPES);
	Assert(io_op < IOOP_NUM_TYPES);
	Assert(pgstat_expect_io_op(MyBackendType, io_context, io_op));

	pending_counters = &pending_IOOpStats.data[io_context];

	switch (io_op)
	{
		case IOOP_CLOCKSWEEP:
			pending_counters->clocksweeps++;
			break;
		case IOOP_EXTEND:
			pending_counters->extends++;
			break;
		case IOOP_FSYNC:
			pending_counters->fsyncs++;
			break;
		case IOOP_HIT:
			pending_counters->hits++;
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
		case IOCONTEXT_LOCAL:
			return "local";
		case IOCONTEXT_SHARED:
			return "shared";
		case IOCONTEXT_VACUUM:
			return "vacuum";
	}

	elog(ERROR, "unrecognized IOContext value: %d", io_context);
}

const char *
pgstat_io_op_desc(IOOp io_op)
{
	switch (io_op)
	{
		case IOOP_CLOCKSWEEP:
			return "clocksweep";
		case IOOP_EXTEND:
			return "extend";
		case IOOP_FSYNC:
			return "fsync";
		case IOOP_HIT:
			return "hit";
		case IOOP_READ:
			return "read";
		case IOOP_REUSE:
			return "reused";
		case IOOP_WRITE:
			return "write";
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
 * Some BackendTypes do not perform IO operations in certain IOContexts. Check
 * that the given BackendType is expected to do IO in the given IOContext.
 */
bool
pgstat_bktype_io_context_valid(BackendType bktype, IOContext io_context)
{
	bool		no_local;

	/*
	 * In core Postgres, only regular backends and WAL Sender processes
	 * executing queries should use local buffers. Parallel workers will not
	 * use local buffers (see InitLocalBuffers()); however, extensions
	 * leveraging background workers have no such limitation, so track IO
	 * Operations in IOCONTEXT_LOCAL for BackendType B_BG_WORKER.
	 */
	no_local = bktype == B_AUTOVAC_LAUNCHER || bktype == B_BG_WRITER || bktype
		== B_CHECKPOINTER || bktype == B_AUTOVAC_WORKER || bktype ==
		B_STANDALONE_BACKEND || bktype == B_STARTUP;

	if (io_context == IOCONTEXT_LOCAL && no_local)
		return false;

	/*
	 * Some BackendTypes do not currently perform any IO operations in certain
	 * IOContexts, and, while it may not be inherently incorrect for them to
	 * do so, excluding those rows from the view makes the view easier to use.
	 */
	if ((io_context == IOCONTEXT_BULKREAD || io_context == IOCONTEXT_BULKWRITE
		 || io_context == IOCONTEXT_VACUUM) && (bktype == B_CHECKPOINTER
												|| bktype == B_BG_WRITER))
		return false;

	if (io_context == IOCONTEXT_VACUUM && bktype == B_AUTOVAC_LAUNCHER)
		return false;

	if (io_context == IOCONTEXT_BULKWRITE && (bktype == B_AUTOVAC_WORKER ||
											  bktype == B_AUTOVAC_LAUNCHER))
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
pgstat_io_op_valid(BackendType bktype, IOContext io_context, IOOp io_op)
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
		(io_op == IOOP_READ || io_op == IOOP_CLOCKSWEEP || io_op == IOOP_HIT))
		return false;

	if ((bktype == B_AUTOVAC_LAUNCHER || bktype == B_BG_WRITER || bktype ==
		 B_CHECKPOINTER) && io_op == IOOP_EXTEND)
		return false;

	/*
	 * Some IOOps are not valid in certain IOContexts
	 */
	if (io_op == IOOP_EXTEND && io_context == IOCONTEXT_BULKREAD)
		return false;

	if (io_op == IOOP_REUSE &&
		(io_context == IOCONTEXT_SHARED || io_context == IOCONTEXT_LOCAL))
		return false;

	/*
	 * Temporary tables using local buffers are not logged and thus do not
	 * require fsync'ing.
	 *
	 * IOOP_FSYNC IOOps done by a backend using a BufferAccessStrategy are
	 * counted in the IOCONTEXT_SHARED IOContext. See comment in
	 * ForwardSyncRequest() for more details.
	 */
	strategy_io_context = io_context == IOCONTEXT_BULKREAD || io_context ==
		IOCONTEXT_BULKWRITE || io_context == IOCONTEXT_VACUUM;

	if ((io_context == IOCONTEXT_LOCAL || strategy_io_context) &&
		io_op == IOOP_FSYNC)
		return false;

	return true;
}

bool
pgstat_expect_io_op(BackendType bktype, IOContext io_context, IOOp io_op)
{
	if (!pgstat_io_op_stats_collected(bktype))
		return false;

	if (!pgstat_bktype_io_context_valid(bktype, io_context))
		return false;

	if (!(pgstat_io_op_valid(bktype, io_context, io_op)))
		return false;

	return true;
}
