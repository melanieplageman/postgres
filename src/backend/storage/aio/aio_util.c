/*-------------------------------------------------------------------------
 *
 * aio_util.c
 *	  Asynchronous I/O subsytem - helpers for using AIO
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/aio/aio_util.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "executor/instrument.h"
#include "storage/aio_internal.h"
#include "miscadmin.h"

#define IOS_LOOKBACK 800

typedef struct IOMovement
{
	instr_time time;
	int num_ios;
} IOMovement;

typedef struct rate_manager
{
	IOMovement ios[IOS_LOOKBACK];
	int ios_idx;
	int num_valid_ios;
} rate_manager;

static void rate_mgr_append_movement(rate_manager *rate_mgr, IOMovement *movement)
{
	IOMovement *target;

	if (rate_mgr->ios_idx == 0)
		rate_mgr->ios_idx = IOS_LOOKBACK;
	rate_mgr->ios_idx--;

	target = &rate_mgr->ios[rate_mgr->ios_idx];

	memcpy(target, movement, sizeof(IOMovement));

	if (rate_mgr->num_valid_ios < IOS_LOOKBACK)
		rate_mgr->num_valid_ios++;
}

void pgsr_log_io_rate_members(io_rate *io_rate)
{
	elog(WARNING, "io_rate->ios: %d. io_rate->duration: microseconds: %f. milliseconds: %f. seconds: %f.",
			io_rate->ios,
			INSTR_TIME_GET_MICROSEC(io_rate->duration),
			INSTR_TIME_GET_MILLISEC(io_rate->duration),
			INSTR_TIME_GET_DOUBLE(io_rate->duration));
}

void pgsr_log_io_rate(io_rate *io_rate)
{
	elog(WARNING, "io_rate: %f IOs/microsec. %f IOs/millisec. %f IOs/sec.",
			pgsr_convert_io_rate_microseconds(io_rate),
			pgsr_convert_io_rate_milliseconds(io_rate),
			pgsr_convert_io_rate_seconds(io_rate));
}

static void pgsr_average_rate(rate_manager *rate_mgr, io_rate *io_rate, bool log)
{
	instr_time time_difference;
	int newest;
	int oldest;

	INSTR_TIME_SET_ZERO(io_rate->duration);
	io_rate->ios = 0;
	if (rate_mgr->num_valid_ios <= 0)
		return;

	INSTR_TIME_SET_ZERO(time_difference);

	newest = rate_mgr->ios_idx;
	oldest = (rate_mgr->ios_idx + rate_mgr->num_valid_ios - 1) % IOS_LOOKBACK;
	INSTR_TIME_ACCUM_DIFF(time_difference, rate_mgr->ios[newest].time, rate_mgr->ios[oldest].time);

	if (oldest < newest)
	{
		for (int i = newest; i < rate_mgr->num_valid_ios; i++)
			io_rate->ios += rate_mgr->ios[i].num_ios;
		for (int i = 0; i < oldest; i++)
			io_rate->ios += rate_mgr->ios[i].num_ios;
	}
	else
	{
		for (int i = newest; i < oldest; i++)
			io_rate->ios += rate_mgr->ios[i].num_ios;
	}


	if (log)
	{
		/* elog(WARNING, "----"); */

		/* for (int k = 0; k < IOS_LOOKBACK; k++) */
		/* { */
		/* 	if (rate_mgr->ios[k].num_ios < 0) */
		/* 		elog(WARNING, "index: %d. time: %f, ios: %i", */
		/* 				k, INSTR_TIME_GET_MICROSEC(rate_mgr->ios[k].time), */
		/* 				rate_mgr->ios[k].num_ios); */
		/* } */

		/* elog(WARNING, "newest idx: %d. oldest idx: %d. ios_idx: %d. num_valid_ios: %d. ios lookback: %d.", */
		/* 		newest, oldest, */
		/* 		rate_mgr->ios_idx, rate_mgr->num_valid_ios, IOS_LOOKBACK); */

	}

	if (INSTR_TIME_GET_MICROSEC(time_difference) <= 0)
		return;

	INSTR_TIME_ADD(io_rate->duration, time_difference);
}

/* typedef is in header */
typedef struct PgStreamingWriteItem
{
	/* membership in PgStreamingWrite->issued, PgStreamingWrite->available */
	dlist_node node;

	PgAioInProgress *aio;
	bool in_purgatory;
	bool in_progress;
	void *private_data;

	struct PgStreamingWrite *pgsw;

	PgStreamingWriteCompleted on_completion_user;
	PgStreamingWriteRetry on_failure_user;

	PgAioOnCompletionLocalContext on_completion_aio;
} PgStreamingWriteItem;

/* typedef is in header */
struct PgStreamingWrite
{
	uint32 iodepth;

	uint32 inflight_count;

	void *private_data;

	/* submitted writes */
	dlist_head issued;

	/* available writes (unused or completed) */
	dlist_head available;

	uint32 max_since_submit;

	/*
	 * IOs returned by pg_streaming_write_get_io, before
	 * pg_streaming_write_release_io or pg_streaming_write_write has been
	 * called.
	 */
	dlist_head purgatory;

	PgStreamingWriteItem all_items[FLEXIBLE_ARRAY_MEMBER];
};


static void pg_streaming_write_complete(PgAioOnCompletionLocalContext *ocb, PgAioInProgress *io);


PgStreamingWrite*
pg_streaming_write_alloc(uint32 iodepth, void *private_data)
{
	PgStreamingWrite *pgsw;

	iodepth = Max(Min(iodepth, NBuffers / 128), 1);

	pgsw = palloc0(offsetof(PgStreamingWrite, all_items)
				 + sizeof(PgStreamingWriteItem) * iodepth);

	pgsw->iodepth = iodepth;
	pgsw->private_data = private_data;

	/*
	 * Submit on a regular basis. Otherwise we'll always have to wait for some
	 * IO to complete when reaching iodepth. But we also want to mostly
	 * achieve large IOs...
	 */
	pgsw->max_since_submit = Max((iodepth / 4) * 3, 1);

	/*
	 * Avoid submission of previous IOs while preparing "our" IO. XXX: this
	 * should be done in a nicer way.
	 */
	if (pgsw->max_since_submit > (PGAIO_SUBMIT_BATCH_SIZE - PGAIO_MAX_COMBINE))
		pgsw->max_since_submit = PGAIO_SUBMIT_BATCH_SIZE - PGAIO_MAX_COMBINE;

	dlist_init(&pgsw->available);
	dlist_init(&pgsw->issued);
	dlist_init(&pgsw->purgatory);

	for (int i = 0; i < pgsw->iodepth; i++)
	{
		PgStreamingWriteItem *this_write = &pgsw->all_items[i];

		this_write->on_completion_aio.callback = pg_streaming_write_complete;
		this_write->pgsw = pgsw;
		dlist_push_tail(&pgsw->available, &this_write->node);
	}

	return pgsw;
}

PgAioInProgress *
pg_streaming_write_get_io(PgStreamingWrite *pgsw)
{
	PgStreamingWriteItem *this_write;

	pgaio_limit_pending(false, pgsw->max_since_submit);

	/* loop in case the callback issues further writes */
	while (dlist_is_empty(&pgsw->available))
	{
		Assert(!dlist_is_empty(&pgsw->issued));

		this_write = dlist_head_element(PgStreamingWriteItem, node, &pgsw->issued);
		Assert(this_write->in_progress);

		pgaio_io_wait(this_write->aio);
		/*
		 * NB: cannot assert that the IO is done now, since the callback may
		 * trigger new IO.
		 */
	}

	this_write = dlist_head_element(PgStreamingWriteItem, node, &pgsw->available);

	Assert(!this_write->in_progress);

	if (!this_write->aio)
	{
		PgAioInProgress *newaio = pgaio_io_get();

		Assert(this_write->aio == NULL);
		this_write->aio = newaio;
	}

	Assert(!this_write->in_progress);
	Assert(!this_write->in_purgatory);
	this_write->in_purgatory = true;
	dlist_delete_from(&pgsw->available, &this_write->node);
	dlist_push_head(&pgsw->purgatory, &this_write->node);

	ereport(DEBUG3, errmsg("pgsw get_io AIO %u/%llu, pgsw %zu, pgsw has now %d inflight",
						   pgaio_io_id(this_write->aio),
						   (long long unsigned) pgaio_io_generation(this_write->aio),
						   this_write - pgsw->all_items,
						   pgsw->inflight_count),
			errhidestmt(true),
			errhidecontext(true));

	return this_write->aio;
}

uint32
pg_streaming_write_inflight(PgStreamingWrite *pgsw)
{
	return pgsw->inflight_count;
}

static PgStreamingWriteItem *
pg_streaming_write_find_purgatory(PgStreamingWrite *pgsw, PgAioInProgress *io)
{
	dlist_iter iter;
	PgStreamingWriteItem *this_write = NULL;

	dlist_foreach(iter, &pgsw->purgatory)
	{
		PgStreamingWriteItem *cur = dlist_container(PgStreamingWriteItem, node, iter.cur);

		Assert(cur->in_purgatory);

		if (cur->aio == io)
		{
			this_write = cur;
			break;
		}
	}

	Assert(this_write);

	return this_write;
}

void
pg_streaming_write_release_io(PgStreamingWrite *pgsw, PgAioInProgress *io)
{
	PgStreamingWriteItem *this_write;

	this_write = pg_streaming_write_find_purgatory(pgsw, io);
	dlist_delete_from(&pgsw->purgatory, &this_write->node);

	this_write->in_purgatory = false;
	dlist_push_tail(&pgsw->available, &this_write->node);


	ereport(DEBUG3, errmsg("pgsw release AIO %u/%llu, pgsw %zu",
						   pgaio_io_id(io),
						   (long long unsigned) pgaio_io_generation(io),
						   this_write - pgsw->all_items),
			errhidestmt(true),
			errhidecontext(true));
}

void
pg_streaming_write_write(PgStreamingWrite *pgsw, PgAioInProgress *io,
						 PgStreamingWriteCompleted on_completion,
						 PgStreamingWriteRetry on_failure,
						 void *private_data)
{
	PgStreamingWriteItem *this_write;

	Assert(pgaio_io_pending(io));
	Assert(!dlist_is_empty(&pgsw->purgatory));

	this_write = pg_streaming_write_find_purgatory(pgsw, io);
	Assert(!this_write->in_progress);
	Assert(this_write->aio && this_write->aio == io);

	this_write->in_purgatory = false;
	dlist_delete_from(&pgsw->purgatory, &this_write->node);

	pgaio_io_on_completion_local(this_write->aio, &this_write->on_completion_aio);

	this_write->on_completion_user = on_completion;
	this_write->on_failure_user = on_failure;
	this_write->private_data = private_data;
	this_write->in_progress = true;

	dlist_push_tail(&pgsw->issued, &this_write->node);
	pgsw->inflight_count++;

	pgaio_limit_pending(false, pgsw->max_since_submit);

	ereport(DEBUG3, errmsg("pgsw write AIO %u/%llu, pgsw %zu, pgsw has now %d inflight",
						   pgaio_io_id(io),
						   (long long unsigned) pgaio_io_generation(io),
						   this_write - pgsw->all_items,
						   pgsw->inflight_count),
			errhidestmt(true),
			errhidecontext(true));
}

static void
pg_streaming_write_complete(PgAioOnCompletionLocalContext *ocb, PgAioInProgress *io)
{
	PgStreamingWriteItem *this_write =
		pgaio_ocb_container(PgStreamingWriteItem, on_completion_aio, ocb);
	PgStreamingWrite *pgsw = this_write->pgsw;
	void *private_data = this_write->private_data;
	int result;

	Assert(this_write->in_progress);
	Assert(!this_write->in_purgatory);
	Assert(pgaio_io_done(io));

	if (!pgaio_io_success(io) && this_write->on_failure_user)
	{
		ereport(DEBUG3,
				errmsg("pgsw completion retry AIO %u/%llu: succ: %d, res: %d, pgsw %zu, pgsw has now %d inflight",
					   pgaio_io_id(io),
					   (long long unsigned) pgaio_io_generation(io),
					   pgaio_io_success(io),
					   pgaio_io_result(io),
					   this_write - pgsw->all_items,
					   pgsw->inflight_count),
				errhidestmt(true),
				errhidecontext(true));

		if (this_write->on_failure_user(pgsw, pgsw->private_data, io, private_data))
		{
			return;
		}
	}

	result = pgaio_io_result(io);

	dlist_delete_from(&pgsw->issued, &this_write->node);
	Assert(pgsw->inflight_count > 0);
	pgsw->inflight_count--;

	this_write->private_data = NULL;
	this_write->in_progress = false;

	ereport(DEBUG3,
			errmsg("pgsw completion AIO %u/%llu: succ: %d, res: %d, pgsw %zu, pgsw has now %d inflight",
				   pgaio_io_id(io),
				   (long long unsigned) pgaio_io_generation(io),
				   pgaio_io_success(io),
				   result,
				   this_write - pgsw->all_items,
				   pgsw->inflight_count),
			errhidestmt(true),
			errhidecontext(true));

	pgaio_io_recycle(this_write->aio);
	dlist_push_tail(&pgsw->available, &this_write->node);

	/* call callback after all other handling so it can issue IO */
	if (this_write->on_completion_user)
		this_write->on_completion_user(pgsw, pgsw->private_data, result, private_data);
}

void
pg_streaming_write_wait_all(PgStreamingWrite *pgsw)
{
	ereport(DEBUG3, errmsg("pgsw wait all, %d inflight",
						   pgsw->inflight_count),
			errhidestmt(true),
			errhidecontext(true));

	while (!dlist_is_empty(&pgsw->issued))
	{
		PgStreamingWriteItem *this_write =
			dlist_head_element(PgStreamingWriteItem, node, &pgsw->issued);

		Assert(this_write->in_progress);
		Assert(!this_write->in_purgatory);
		pgaio_io_wait(this_write->aio);
	}

	Assert(pgsw->inflight_count == 0);
}

void
pg_streaming_write_free(PgStreamingWrite *pgsw)
{
	for (uint32 off = 0; off < pgsw->iodepth; off++)
	{
		PgStreamingWriteItem *this_write = &pgsw->all_items[off];

		Assert(!this_write->in_progress);
		Assert(!this_write->in_purgatory);
		if (this_write->aio)
			pgaio_io_release(this_write->aio);
		this_write->aio = NULL;
	}

	pfree(pgsw);
}

typedef struct PgStreamingReadItem
{
	/* membership in PgStreamingRead->issued, PgStreamingRead->available */
	dlist_node node;
	/* membership in PgStreamingRead->in_order */
	dlist_node sequence_node;

	PgAioOnCompletionLocalContext on_completion;
	PgStreamingRead *pgsr;
	PgAioInProgress *aio;

	/* is this IO still considered to be in progress */
	bool in_progress;
	/* is this item currently valid / used */
	bool valid;
	uintptr_t read_private;
} PgStreamingReadItem;

struct PgStreamingRead
{
	uint32 iodepth_max;
	uint32 distance_max;
	uint32 all_items_count;

	uintptr_t pgsr_private;
	PgStreamingReadDetermineNextCB determine_next_cb;
	PgStreamingReadRelease release_cb;

	uint32 current_window;

	/* number of requests issued */
	uint64 prefetched_total_count;
	/* number of requests submitted to kernel */
	uint64 submitted_total_count;
	/* number of current requests completed */
	uint32 completed_count;
	/* number of current requests in flight */
	int32 inflight_count;
	/* number of requests that didn't require IO (debugging only) */
	int32 no_io_count;

	bool hit_end;

	rate_manager demand;
	rate_manager prefetch;
	io_rate demand_rate;

	bool log;

	double _rate;
	instr_time last_prefetch_time;
	uint64 prefetched_last_time;
	double maximum_volume;
	double volume;

	bool adjusted_prefetch;

	/* submitted reads */
	dlist_head issued;

	/* available reads (unused or completed) */
	dlist_head available;

	/*
	 * IOs, be they completed or in progress, in the order that the callback
	 * returned them.
	 */
	dlist_head in_order;

	PgStreamingReadItem all_items[FLEXIBLE_ARRAY_MEMBER];
};

static void pg_streaming_read_complete(PgAioOnCompletionLocalContext *ocb, PgAioInProgress *io);
static void pg_streaming_read_prefetch(PgStreamingRead *pgsr);

PgStreamingRead *
pg_streaming_read_alloc(uint32 iodepth, double starting_prefetch_rate,
						uintptr_t pgsr_private,
						PgStreamingReadDetermineNextCB determine_next_cb,
						PgStreamingReadRelease release_cb)
{
	PgStreamingRead *pgsr;

	iodepth = Max(Min(iodepth, NBuffers / 128), 1);
	/* elog(WARNING, "iodepth is %d", iodepth); */

	pgsr = palloc0(offsetof(PgStreamingRead, all_items) +
				   sizeof(PgStreamingReadItem) * iodepth * 2);

	pgsr->iodepth_max = iodepth;
	pgsr->distance_max = iodepth;
	pgsr->all_items_count = pgsr->iodepth_max + pgsr->distance_max;
	pgsr->pgsr_private = pgsr_private;
	pgsr->determine_next_cb = determine_next_cb;
	pgsr->release_cb = release_cb;

	pgsr->current_window = 0;

	dlist_init(&pgsr->available);
	dlist_init(&pgsr->in_order);
	dlist_init(&pgsr->issued);

	pgsr->demand.ios_idx = 0;
	pgsr->demand.num_valid_ios = 0;

	pgsr->demand_rate.ios = 0;
	INSTR_TIME_SET_ZERO(pgsr->demand_rate.duration);

	pgsr->prefetch.ios_idx = 0;
	pgsr->prefetch.num_valid_ios = 0;

	/* assumed to be IOs/second. convert to microsecond */
	pgsr->_rate = (double) starting_prefetch_rate / 1000000;
	pgsr->adjusted_prefetch = false;
	pgsr->log = false;

	INSTR_TIME_SET_ZERO(pgsr->last_prefetch_time);
	pgsr->prefetched_last_time = 0;
	pgsr->volume = 0;
	pgsr->prefetched_total_count = 0;

	/* don't build up more than this many IOs in remnants */
	pgsr->maximum_volume = 1000;

	for (int i = 0; i < pgsr->all_items_count; i++)
	{
		PgStreamingReadItem *this_read = &pgsr->all_items[i];

		this_read->on_completion.callback = pg_streaming_read_complete;
		this_read->pgsr = pgsr;
		dlist_push_tail(&pgsr->available, &this_read->node);
	}

	/* elog(WARNING, "Allocated"); */

	return pgsr;
}

void
pg_streaming_read_free(PgStreamingRead *pgsr)
{
	/*
	 * Prevent further read-ahead from being queued in the completion
	 * callback. We'd not necessarily wait for them in this loop, leaving
	 * their completion callbacks point to already freed memory.
	 */
	pgsr->hit_end = true;

	for (int i = 0; i < pgsr->all_items_count; i++)
	{
		PgStreamingReadItem *this_read = &pgsr->all_items[i];

		if (this_read->in_progress)
		{
			Assert(this_read->valid);
			pgaio_io_wait(this_read->aio);
			Assert(!this_read->in_progress);
		}

		if (this_read->valid)
			pgsr->release_cb(pgsr->pgsr_private, this_read->read_private);

		if (this_read->aio)
		{
			pgaio_io_release(this_read->aio);
			this_read->aio = NULL;
		}
	}

	pfree(pgsr);
}

static void
pg_streaming_read_complete(PgAioOnCompletionLocalContext *ocb, PgAioInProgress *io)
{
	PgStreamingReadItem *this_read = pgaio_ocb_container(PgStreamingReadItem, on_completion, ocb);
	PgStreamingRead *pgsr = this_read->pgsr;

#if 0
	if ((pgsr->prefetched_total_count % 10000) == 0)
		ereport(LOG, errmsg("pgsr read completed: qd %d completed: %d",
							pgsr->inflight_count, pgsr->completed_count),
				errhidestmt(true),
				errhidecontext(true));
#endif

	Assert(this_read->in_progress);
	Assert(this_read->valid);
	Assert(this_read->aio == io);
	Assert(pgsr->inflight_count > 0);
	Assert(pgaio_io_done(io));
	Assert(pgaio_io_success(io));

	dlist_delete_from(&pgsr->issued, &this_read->node);
	pgsr->inflight_count--;
	pgsr->completed_count++;
	this_read->in_progress = false;
	pgaio_io_recycle(this_read->aio);

	pg_streaming_read_prefetch(pgsr);
}

static PgStreamingReadNextStatus
pg_streaming_read_prefetch_one(PgStreamingRead *pgsr, instr_time *time)
{
	PgStreamingReadItem *this_read;
	PgStreamingReadNextStatus status;

	Assert(!dlist_is_empty(&pgsr->available));

	this_read = dlist_container(PgStreamingReadItem, node, dlist_pop_head_node(&pgsr->available));
	Assert(!this_read->valid);
	Assert(!this_read->in_progress);
	Assert(this_read->read_private == 0);

	if (this_read->aio == NULL)
	{
		this_read->aio = pgaio_io_get();
	}

	pgaio_io_on_completion_local(this_read->aio, &this_read->on_completion);
	this_read->in_progress = true;
	this_read->valid = true;
	dlist_push_tail(&pgsr->issued, &this_read->node);
	dlist_push_tail(&pgsr->in_order, &this_read->sequence_node);
	pgsr->inflight_count++;
	pgsr->prefetched_total_count++;

	status = pgsr->determine_next_cb(pgsr->pgsr_private, this_read->aio, &this_read->read_private);

	if (status == PGSR_NEXT_END)
	{
		pgsr->inflight_count--;
		pgsr->prefetched_total_count--;
		pgsr->hit_end = true;
		this_read->read_private = 0;
		this_read->valid = false;
		this_read->in_progress = false;
		pgaio_io_recycle(this_read->aio);
		dlist_delete_from(&pgsr->in_order, &this_read->sequence_node);
		dlist_push_tail(&pgsr->available, &this_read->node);

	}
	else if (status == PGSR_NEXT_NO_IO)
	{
		Assert(this_read->read_private != 0);
		pgsr->inflight_count--;
		pgsr->no_io_count++;
		pgsr->completed_count++;
		this_read->in_progress = false;
		pgaio_io_recycle(this_read->aio);
		dlist_delete_from(&pgsr->issued, &this_read->node);
	}
	else
	{
		IOMovement movement;

		INSTR_TIME_SET_ZERO(movement.time);
		INSTR_TIME_ADD(movement.time, *time);
		Assert(this_read->read_private != 0);
		movement.num_ios = 1;
		rate_mgr_append_movement(&pgsr->prefetch, &movement);
	}

	return status;
}

static void
pg_streaming_read_prefetch(PgStreamingRead *pgsr)
{
	instr_time time_elapsed;
	int to_move;

	pgsr->prefetched_last_time = 0;
	if (pgsr->hit_end)
		return;

	/*
	 * The prefetch rate may be driven to 0 in an attempt to slow down the
	 * number of IOs being requested. In this case, set the volume to 0 and
	 * return.
	 */
	if (pgsr->_rate == 0)
	{
		pgsr->volume = 0;
		return;
	}

	INSTR_TIME_SET_ZERO(time_elapsed);

	/*
	 * If we have prefetched before, calculate how much time has elapsed since
	 * we last prefetched.
	 */
	if (!INSTR_TIME_IS_ZERO(pgsr->last_prefetch_time))
	{
		instr_time current_time;
		INSTR_TIME_SET_CURRENT(current_time);
		INSTR_TIME_ACCUM_DIFF(time_elapsed, current_time, pgsr->last_prefetch_time);
	}

	pgsr->volume += INSTR_TIME_GET_MICROSEC(time_elapsed) * pgsr->_rate;
	pgsr->volume = Min(pgsr->volume, pgsr->maximum_volume);

	if (pgsr->volume < 0)
		to_move = 0;
	else
		to_move = (int) pgsr->volume;
	Assert(to_move >= 0);

	if (to_move == 0 && (pgsr->inflight_count == 0 || pgsr->completed_count == 0))
		to_move = 1;

	if (pgsr->log)
	{
		/* elog(WARNING, "%p: to_move: %u. pgsr->_rate: %f. pgsr->volume: %f. pgsr->maximum_volume: %f. time elapsed microsec: %f. prefetched last time: %ld", */
		/* 		pgsr, to_move, pgsr->_rate, pgsr->volume, pgsr->maximum_volume, INSTR_TIME_GET_MICROSEC(time_elapsed), pgsr->prefetched_last_time); */

		/* elog(WARNING, "inflight: %d. completed: %d.", pgsr->inflight_count, pgsr->completed_count); */
	}

	if (pgsr->prefetched_total_count > 20 && pgsr->adjusted_prefetch == false)
	{
		double demand_rate;
		pgsr->adjusted_prefetch = true;
		demand_rate = pg_streaming_read_demand_rate(pgsr, false, 1);
		if (pgsr->log)
			elog(WARNING, "setting prefetch rate to %f", demand_rate);
		pgsr->_rate = demand_rate;
	}

	/* if (to_move > 0) */
	INSTR_TIME_SET_CURRENT(pgsr->last_prefetch_time);

	while (!pgsr->hit_end && to_move > 0)
	{
		uint32 min_issue = 1;
		if (pg_streaming_read_prefetch_one(pgsr, &pgsr->last_prefetch_time) == PGSR_NEXT_IO)
			pgsr->prefetched_last_time++;

		pgaio_limit_pending(false, min_issue);
		to_move--;

		CHECK_FOR_INTERRUPTS();

		/* if (pgsr->hit_end) */
		/* 	elog(WARNING, "Hit end"); */
	}

	/* Deduct how many we actually prefetched */
	pgsr->volume -= pgsr->prefetched_last_time;
}

double pgsr_convert_io_rate_microseconds(io_rate *io_rate)
{
	if (INSTR_TIME_IS_ZERO(io_rate->duration))
		return 0;

	return (double) io_rate->ios / INSTR_TIME_GET_MICROSEC(io_rate->duration);
}

double pgsr_convert_io_rate_milliseconds(io_rate *io_rate)
{
	if (INSTR_TIME_IS_ZERO(io_rate->duration))
		return 0;

	return (double) io_rate->ios / INSTR_TIME_GET_MILLISEC(io_rate->duration);
}

double pgsr_convert_io_rate_seconds(io_rate *io_rate)
{
	if (INSTR_TIME_IS_ZERO(io_rate->duration))
		return 0;

	return (double) io_rate->ios / INSTR_TIME_GET_DOUBLE(io_rate->duration);
}

double
pg_streaming_read_demand_rate(PgStreamingRead *pgsr, bool log, int headroom)
{
	io_rate rate;
	double converted_demand_rate;

	pgsr_average_rate(&pgsr->demand, &rate, log);

	/* if (log) */
	/* 	pgsr_log_io_rate_members(&rate); */

	rate.ios += headroom;

	converted_demand_rate = pgsr_convert_io_rate_microseconds(&rate);

	if (log)
	{
		if (converted_demand_rate <= 0)
			elog(WARNING, "No demand");
		else
		{
			/* elog(WARNING, "Old demand rate:"); */
			/* pgsr_log_io_rate(&pgsr->demand_rate); */

			elog(WARNING, "New demand rate");
			pgsr_log_io_rate(&rate);
		}
	}

	memcpy(&pgsr->demand_rate, &rate, sizeof(io_rate));

	return converted_demand_rate;
}

double
pg_streaming_read_prefetch_rate(PgStreamingRead *pgsr, bool log)
{
	io_rate rate;
	double converted_prefetch_rate;

	pgsr_average_rate(&pgsr->prefetch, &rate, log);

	/* if (log) */
	/* 	pgsr_log_io_rate_members(&rate); */

	converted_prefetch_rate = pgsr_convert_io_rate_microseconds(&rate);

	if (log)
	{
		if (converted_prefetch_rate <= 0)
			elog(WARNING, "No prefetching done");
		else
		{
			elog(WARNING, "Prefetch rate:");
			pgsr_log_io_rate(&rate);
		}
	}

	return converted_prefetch_rate;
}

void pgsr_set_log(PgStreamingRead *pgsr, bool log)
{
	pgsr->log = log;
}

uintptr_t
pg_streaming_read_get_next(PgStreamingRead *pgsr)
{
	IOMovement movement;
	INSTR_TIME_SET_CURRENT(movement.time);

	if (pgsr->prefetched_total_count == 0)
	{
		pg_streaming_read_prefetch(pgsr);
		Assert(pgsr->hit_end || pgsr->prefetched_total_count > 0);
	}

	if (dlist_is_empty(&pgsr->in_order))
	{
		/* Assert(pgsr->hit_end); */
		return 0;
	}
	else
	{
		PgStreamingReadItem *this_read;
		uint64_t ret;

		Assert(pgsr->prefetched_total_count > 0);

		this_read = dlist_container(PgStreamingReadItem, sequence_node,
									dlist_pop_head_node(&pgsr->in_order));
		Assert(this_read->valid);

		// append movement if this_read was real IO
		if (this_read->in_progress)
		{
			pgaio_io_wait(this_read->aio);
			/* callback should have updated */
			Assert(!this_read->in_progress);
			Assert(this_read->valid);
		}

		Assert(this_read->read_private != 0);
		ret = this_read->read_private;
		this_read->read_private = 0;
		this_read->valid = false;

		pgsr->completed_count--;
		dlist_push_tail(&pgsr->available, &this_read->node);
		pg_streaming_read_prefetch(pgsr);

		movement.num_ios = 1;
		rate_mgr_append_movement(&pgsr->demand, &movement);

		return ret;
	}
}
