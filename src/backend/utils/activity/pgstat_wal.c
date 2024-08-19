/* -------------------------------------------------------------------------
 *
 * pgstat_wal.c
 *	  Implementation of WAL statistics.
 *
 * This file contains the implementation of WAL statistics. It is kept
 * separate from pgstat.c to enforce the line between the statistics access /
 * storage implementation and the details about individual types of
 * statistics.
 *
 * Copyright (c) 2001-2024, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *	  src/backend/utils/activity/pgstat_wal.c
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

#include "access/xlog.h"
#include "access/xact.h"
#include "access/xloginsert.h"
#include "executor/instrument.h"
#include "math.h"
#include "utils/pgstat_internal.h"
#include "utils/timestamp.h"
#include "inttypes.h"


PgStat_PendingWalStats PendingWalStats = {0};

/*
 * WAL usage counters saved from pgWalUsage at the previous call to
 * pgstat_report_wal(). This is used to calculate how much WAL usage
 * happens between pgstat_report_wal() calls, by subtracting
 * the previous counters from the current ones.
 */
static WalUsage prevWalUsage;

/*
 * LSNTimeStream maintenance helper functions
 */
static uint32 lsntime_to_drop(LSNTimeStream *stream);

static void lsntime_insert(LSNTimeStream *stream, TimestampTz time,
						   XLogRecPtr lsn);

/*
 * LSNTimeStream usage helper function
 */
pg_attribute_unused()
static void stream_get_bounds_for_time(const LSNTimeStream *stream,
									   TimestampTz target_time,
									   LSNTime *lower,
									   LSNTime *upper);


/*
 * Calculate how much WAL usage counters have increased and update
 * shared WAL and IO statistics.
 *
 * Must be called by processes that generate WAL, that do not call
 * pgstat_report_stat(), like walwriter.
 *
 * "force" set to true ensures that the statistics are flushed; note that
 * this needs to acquire the pgstat shmem LWLock, waiting on it.  When
 * set to false, the statistics may not be flushed if the lock could not
 * be acquired.
 */
void
pgstat_report_wal(bool force)
{
	bool		nowait;

	/* like in pgstat.c, don't wait for lock acquisition when !force */
	nowait = !force;

	/* flush wal stats */
	pgstat_flush_wal(nowait);

	/* flush IO stats */
	pgstat_flush_io(nowait);
}

/*
 * Support function for the SQL-callable pgstat* functions. Returns
 * a pointer to the WAL statistics struct.
 */
PgStat_WalStats *
pgstat_fetch_stat_wal(void)
{
	pgstat_snapshot_fixed(PGSTAT_KIND_WAL);

	return &pgStatLocal.snapshot.wal;
}

/*
 * Calculate how much WAL usage counters have increased by subtracting the
 * previous counters from the current ones.
 *
 * If nowait is true, this function returns true if the lock could not be
 * acquired. Otherwise return false.
 */
bool
pgstat_flush_wal(bool nowait)
{
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;
	WalUsage	wal_usage_diff = {0};

	Assert(IsUnderPostmaster || !IsPostmasterEnvironment);
	Assert(pgStatLocal.shmem != NULL &&
		   !pgStatLocal.shmem->is_shutdown);

	/*
	 * This function can be called even if nothing at all has happened. Avoid
	 * taking lock for nothing in that case.
	 */
	if (!pgstat_have_pending_wal())
		return false;

	/*
	 * We don't update the WAL usage portion of the local WalStats elsewhere.
	 * Calculate how much WAL usage counters were increased by subtracting the
	 * previous counters from the current ones.
	 */
	WalUsageAccumDiff(&wal_usage_diff, &pgWalUsage, &prevWalUsage);

	if (!nowait)
		LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
	else if (!LWLockConditionalAcquire(&stats_shmem->lock, LW_EXCLUSIVE))
		return true;

#define WALSTAT_ACC(fld, var_to_add) \
	(stats_shmem->stats.fld += var_to_add.fld)
#define WALSTAT_ACC_INSTR_TIME(fld) \
	(stats_shmem->stats.fld += INSTR_TIME_GET_MICROSEC(PendingWalStats.fld))
	WALSTAT_ACC(wal_records, wal_usage_diff);
	WALSTAT_ACC(wal_fpi, wal_usage_diff);
	WALSTAT_ACC(wal_bytes, wal_usage_diff);
	WALSTAT_ACC(wal_buffers_full, PendingWalStats);
	WALSTAT_ACC(wal_write, PendingWalStats);
	WALSTAT_ACC(wal_sync, PendingWalStats);
	WALSTAT_ACC_INSTR_TIME(wal_write_time);
	WALSTAT_ACC_INSTR_TIME(wal_sync_time);
#undef WALSTAT_ACC_INSTR_TIME
#undef WALSTAT_ACC

	LWLockRelease(&stats_shmem->lock);

	/*
	 * Save the current counters for the subsequent calculation of WAL usage.
	 */
	prevWalUsage = pgWalUsage;

	/*
	 * Clear out the statistics buffer, so it can be re-used.
	 */
	MemSet(&PendingWalStats, 0, sizeof(PendingWalStats));

	return false;
}

void
pgstat_init_wal(void)
{
	/*
	 * Initialize prevWalUsage with pgWalUsage so that pgstat_flush_wal() can
	 * calculate how much pgWalUsage counters are increased by subtracting
	 * prevWalUsage from pgWalUsage.
	 */
	prevWalUsage = pgWalUsage;
}

/*
 * To determine whether any WAL activity has occurred since last time, not
 * only the number of generated WAL records but also the numbers of WAL
 * writes and syncs need to be checked. Because even transaction that
 * generates no WAL records can write or sync WAL data when flushing the
 * data pages.
 */
bool
pgstat_have_pending_wal(void)
{
	return pgWalUsage.wal_records != prevWalUsage.wal_records ||
		PendingWalStats.wal_write != 0 ||
		PendingWalStats.wal_sync != 0;
}

void
pgstat_wal_init_shmem_cb(void *stats)
{
	PgStatShared_Wal *stats_shmem = (PgStatShared_Wal *) stats;

	LWLockInitialize(&stats_shmem->lock, LWTRANCHE_PGSTATS_DATA);
}

void
pgstat_wal_reset_all_cb(TimestampTz ts)
{
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;

	LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
	memset(&stats_shmem->stats, 0, sizeof(stats_shmem->stats));
	stats_shmem->stats.stat_reset_timestamp = ts;
	LWLockRelease(&stats_shmem->lock);
}

void
pgstat_wal_snapshot_cb(void)
{
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;

	LWLockAcquire(&stats_shmem->lock, LW_SHARED);
	memcpy(&pgStatLocal.snapshot.wal, &stats_shmem->stats,
		   sizeof(pgStatLocal.snapshot.wal));
	LWLockRelease(&stats_shmem->lock);
}

/*
 * Returns the index of the LSNTime to drop from a full LSNTimeStream.
 *
 * We find the oldest element that is not at capacity and drop the subsequent
 * LSNTime. If all elements are at capacity drop the oldest LSNTime. This
 * provides the highest accuracy for recent data. See comments on
 * LSNTIMESTREAM_CAPACITY_REPEATS for more on how element capacity is
 * determined.
 */
static uint32
lsntime_to_drop(LSNTimeStream *stream)
{
	/* Don't drop LSNTimes if the array is not full */
	Assert(stream->length == LSNTIMESTREAM_VOLUME);

	for (size_t i = 0; i < LSNTIMESTREAM_VOLUME - 1; i++)
	{
		uint32		section = i / LSNTIMESTREAM_CAPACITY_REPEATS;
		uint32		capacity = 1 << (LSNTIMESTREAM_NSECTIONS - section - 1);

		if (stream->data[i].members >= capacity)
			continue;

		stream->data[i].members++;
		return i + 1;
	}

	/*
	 * Once all of the elements have reached capacity, the oldest LSNTime is
	 * dropped.
	 */
	return 0;
}

/*
 * Insert a new LSNTime with the passed-in time and lsn into the LSNTimeStream
 * in the first available element. If there are no empty elements, drop an
 * LSNTime from the stream to make room for the new LSNTime.
 */
static void
lsntime_insert(LSNTimeStream *stream, TimestampTz time,
			   XLogRecPtr lsn)
{
	uint32		drop;
	LSNTime		entrant = LSNTIME_INIT(lsn, time);

	if (stream->length < LSNTIMESTREAM_VOLUME)
	{
		/*
		 * Time must move forward on the stream. If the clock moves backwards,
		 * for example in an NTP correction, we'll just skip inserting this
		 * LSNTime.
		 *
		 * Though time must monotonically increase, it is valid to insert
		 * multiple LSNTimes with the same LSN. Imagine a period of time in
		 * which no new WAL records are inserted.
		 */
		if (stream->length > 0 &&
			(time <= stream->data[stream->length - 1].time ||
			 lsn < stream->data[stream->length - 1].lsn))
		{
			ereport(WARNING,
					errmsg("Won't insert non-monotonic \"%" PRIu64 ", %s\" to LSNTimeStream.",
						   lsn, timestamptz_to_str(time)));
			return;
		}

		stream->data[stream->length++] = entrant;
		return;
	}

	drop = lsntime_to_drop(stream);

	/*
	 * If we are dropping any LSNTime except the most recent, shift the data
	 * down.
	 */
	if (drop < stream->length - 1)
		memmove(&stream->data[drop],
				&stream->data[drop + 1],
				sizeof(LSNTime) * (stream->length - 1 - drop));

	stream->data[stream->length - 1] = entrant;
}


/*
 * Utility function for inserting a new member into the LSNTimeStream member
 * of WAL stats.
 */
void
pgstat_wal_update_lsntime_stream(XLogRecPtr lsn, TimestampTz time)
{
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;
	Assert(!RecoveryInProgress());

	log_lsntime(lsn, time);
	LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
	lsntime_insert(&stats_shmem->stats.stream, time, lsn);
	LWLockRelease(&stats_shmem->lock);
}


/*
 * Emit a WAL record with the provided LSN and time. When inserting a new
 * LSNTime to the LSNTimeStream, log it to provide durability and ensure a
 * newly promoted standby has an accurate stream.
 */
void
log_lsntime(XLogRecPtr lsn, TimestampTz time)
{
	xl_lsntime xlrec;

	xlrec.time = time;
	xlrec.lsn = lsn;

	XLogBeginInsert();
	XLogRegisterData((char *) (&xlrec), SizeOfLSNTime);
	(void) XLogInsert(RM_LSNTIME_ID, XLOG_LSNTIME);
}

/*
 * Insert a previously logged LSNTime into the LSNTimeStream member of WAL
 * stats. The LSNTimeStream is logged to provide durability and to ensure a
 * newly promoted primary has the same LSNTimeStream as the primary that it is
 * replacing.
 */
void
lsntimestream_redo(XLogReaderState *record)
{
	uint8		info PG_USED_FOR_ASSERTS_ONLY;
	xl_lsntime *lsnt_rec;
	PgStatShared_Wal *stats_shmem = &pgStatLocal.shmem->wal;

	info = XLogRecGetInfo(record) & ~XLR_INFO_MASK;
	Assert(info == XLOG_LSNTIME);

	lsnt_rec = (xl_lsntime *) XLogRecGetData(record);

	LWLockAcquire(&stats_shmem->lock, LW_EXCLUSIVE);
	lsntime_insert(&stats_shmem->stats.stream, lsnt_rec->time, lsnt_rec->lsn);
	LWLockRelease(&stats_shmem->lock);
}


/*
 * Returns (into *lower and *upper) the narrowest range of LSNTimes on the
 * stream covering the target_time.
 *
 * If target_time is older than all values on the stream, lower will be
 * invalid. If target_time is newer than all values on the stream, upper will
 * be invalid. If the stream is empty, both upper and lower will be invalid.
 * The caller must check if upper and/or lower's lsn member is
 * InvalidXLogRecPtr to detect whether that bound is valid.
 */
static void
stream_get_bounds_for_time(const LSNTimeStream *stream,
						   TimestampTz target_time,
						   LSNTime *lower,
						   LSNTime *upper)
{
	Assert(lower && upper);

	*lower = LSNTIME_INIT(InvalidXLogRecPtr, 0);
	*upper = LSNTIME_INIT(InvalidXLogRecPtr, 0);

	/*
	 * If the LSNTimeStream has no members, it provides no information about
	 * the range.
	 */
	if (stream->length == 0)
	{
		elog(DEBUG1,
			 "Attempt to identify LSN bounds for time: \"%s\" using empty LSNTimeStream.",
			 timestamptz_to_str(target_time));
		return;
	}

	/*
	 * If the target_time is older than the stream, the oldest member in the
	 * stream is our upper bound.
	 */
	if (target_time <= stream->data[0].time)
	{
		*upper = stream->data[0];
		if (target_time == stream->data[0].time)
			*lower = stream->data[0];
		return;
	}

	/*
	 * Loop through the stream and stop at the first LSNTime newer than or
	 * equal to our target time. Skip the first LSNTime, as we know it is
	 * older than our target time.
	 */
	for (size_t i = 1; i < stream->length; i++)
	{
		if (target_time == stream->data[i].time)
		{
			*lower = stream->data[i];
			*upper = stream->data[i];
			return;
		}

		if (target_time < stream->data[i].time)
		{
			/* Time must increase monotonically on the stream. */
			Assert(stream->data[i - 1].time <
				   stream->data[i].time);
			*lower = stream->data[i - 1];
			*upper = stream->data[i];
			return;
		}
	}

	/*
	 * target_time is newer than the stream, so the newest member in the
	 * stream is our lower bound.
	 */
	*lower = stream->data[stream->length - 1];
}
