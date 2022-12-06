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

#include "storage/aio_internal.h"
#include "miscadmin.h"
#include "executor/instrument.h"


bool pgsr_do_log = false;

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

	/* prefetch distance at the time this IO was prefetched */
	uint32 prefetch_distance;

	/* Whether or not this block was fetched from disk */
	bool real_io;

	/* completed - submitted for this IO. not valid for cached IOs */
	instr_time latency;

	/* time at which this IO was consumed by a worker */
	instr_time consumed;
} PgStreamingReadItem;

struct PgStreamingRead
{
	uint32 iodepth_max;
	uint32 distance_max;
	uint32 all_items_count;

	uintptr_t pgsr_private;
	PgStreamingReadDetermineNextCB determine_next_cb;
	PgStreamingReadRelease release_cb;
	PgStreamingReadDevLog *dev_log;

	PgStreamingReadConsumptionRing *consumptions;

	uint32 current_window;
	int cnc_lo;
	int cnc_hi;
	int pfd_at_flip;
	int consecutive_ups;
	int consecutive_downs;
	int local_pfd_max;

	/*
	 * this is the number of IOs we are planning to keep inflight at a given
	 * time -- we count those we are about to submit plus those already
	 * inflight when deciding how much to prefetch
	 */
	uint32 prefetch_distance;

	/* this is the highest we should ever set prefetch_distance */
	uint32 max_ios;

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

/*
 * returns true if it changed pfd and false if not
 * sets did_calculation to true if it did the more intense calculations and
 * false if not
 */
static bool pg_streaming_read_adjust_pfd(PgStreamingReadItem *this_read, bool *did_calculation)
{
	float io_tput;
	float tput_ratio;
	float avg_tput;
	float avg_pfd;
	int io_pfd;
	int max_pfd;
	bool changed_pfd = false;
	PgStreamingRead *pgsr = this_read->pgsr;
	int initial_pfd = pgsr->prefetch_distance;

	*did_calculation = false;

	if (!pgsr->consumptions)
		return changed_pfd;

	/* FOR FAST STORAGE */
	if (pgsr->completed_count <= pgsr->cnc_lo)
		pgsr->prefetch_distance += 1;

	if (pgsr->completed_count >= pgsr->cnc_hi)
		pgsr->prefetch_distance = Max(1, pgsr->prefetch_distance - 1);

	if (pgsr->prefetch_distance != initial_pfd)
		changed_pfd = true;

	if (!this_read->real_io)
		return changed_pfd;

	/* FOR SLOW STORAGE */
	io_pfd = this_read->prefetch_distance;
	avg_pfd = pgsr_avg_pfd(pgsr->consumptions);
	max_pfd = pgsr_max_pfd(pgsr->consumptions);

	io_tput = io_pfd / INSTR_TIME_GET_MILLISEC(this_read->latency);
	avg_tput = pgsr_avg_tput(pgsr->consumptions);

	tput_ratio = io_tput / avg_tput;

	*did_calculation = true;
	/*
	 * if pfd is going up and tput is going up this is a possible value to
	 * roll back to
	 */
	if (io_pfd > avg_pfd && tput_ratio >= 1.1)
	{
		pgsr->pfd_at_flip = io_pfd;
		pgsr->consecutive_ups++;
		pgsr->consecutive_downs = 0;

		/*
		 *  If we have had enough ups in a row, invalidate our maximum for
		 *  now
		 */
		if (pgsr->consecutive_ups >= avg_pfd)
			pgsr->local_pfd_max = -1;

		/*
		 *  if we have previously set a maximum and we haven't had enough ups
		 *  in a row, anyway increase the max so that we are able to continue
		 *  increasing the prefetch distance
		 */
		if (pgsr->local_pfd_max >= 0)
			pgsr->local_pfd_max = Min(pgsr->max_ios, pgsr->local_pfd_max + 1);
	}

	/*
	 * if pfd is going up and tput is not going up
	 */
	else if (io_pfd > avg_pfd && tput_ratio < 1.1)
	{
		pgsr->consecutive_downs++;
		pgsr->consecutive_ups = 0;

		/*
		 * If we have had enough downs in a row, set a maximum and set pfd
		 * to a bit less than what it was last time we had an increase in
		 * tput
		 */
		if (pgsr->consecutive_downs >= avg_pfd)
		{
			/*
			 * Set maximum to recent max pfd out of consumed IOs. At this
			 * point, self.prefetch_distance could be higher but we know
			 * that just out of consumed IOs, a pfd that high wasn't
			 * helping us. It may be necessary to actually set this to
			 * self.prefetch_distance to avoid getting stuck at too low of
			 * a value, though
			 */
			pgsr->local_pfd_max     = Max(Min(max_pfd, pgsr->max_ios), 0);
			pgsr->prefetch_distance = pgsr->pfd_at_flip * 0.7;
		}

		/*
		 * counteract the increase from algo 1 even if we haven't had
		 * enuogh consecutive downs
		 */
		pgsr->prefetch_distance -= 1;
	}

	pgsr->prefetch_distance = Max(pgsr->prefetch_distance, 1);

	if (pgsr->local_pfd_max >= 0)
		pgsr->prefetch_distance = Min(pgsr->local_pfd_max, pgsr->prefetch_distance);

	pgsr->prefetch_distance = Min(pgsr->prefetch_distance, pgsr->max_ios);

	if (pgsr->prefetch_distance != initial_pfd)
		changed_pfd = true;

	return changed_pfd;
}

static void pg_streaming_read_complete(PgAioOnCompletionLocalContext *ocb, PgAioInProgress *io);
static void pg_streaming_read_prefetch(PgStreamingRead *pgsr);

PgStreamingRead *
pg_streaming_read_alloc(uint32 iodepth, uintptr_t pgsr_private,
						PgStreamingReadDetermineNextCB determine_next_cb,
						PgStreamingReadRelease release_cb,
						PgStreamingReadConsumptionRing *consumption_ring)
{
	PgStreamingRead *pgsr;

	iodepth = Max(Min(iodepth, NBuffers / 128), 1);

	pgsr = palloc0(offsetof(PgStreamingRead, all_items) +
				   sizeof(PgStreamingReadItem) * iodepth * 2);

	pgsr->iodepth_max = iodepth;
	pgsr->distance_max = iodepth;
	pgsr->all_items_count = pgsr->iodepth_max + pgsr->distance_max;
	pgsr->max_ios = iodepth;
	pgsr->pgsr_private = pgsr_private;
	pgsr->determine_next_cb = determine_next_cb;
	pgsr->release_cb = release_cb;

	pgsr->current_window = 0;
	pgsr->cnc_lo = Min(iodepth, 2);
	pgsr->cnc_hi = Min(iodepth, 15);
	pgsr->consecutive_ups = 0;
	pgsr->consecutive_downs = 0;
	pgsr->pfd_at_flip   = 0;
	pgsr->local_pfd_max = -1;

	pgsr->prefetch_distance = pgsr->cnc_lo;

	dlist_init(&pgsr->available);
	dlist_init(&pgsr->in_order);
	dlist_init(&pgsr->issued);

	for (int i = 0; i < pgsr->all_items_count; i++)
	{
		PgStreamingReadItem *this_read = &pgsr->all_items[i];

		this_read->on_completion.callback = pg_streaming_read_complete;
		this_read->pgsr = pgsr;
		dlist_push_tail(&pgsr->available, &this_read->node);
	}

	pgsr->dev_log = NULL;

	pgsr->consumptions = consumption_ring;

	return pgsr;
}

void
pg_streaming_read_set_dev_log(PgStreamingRead *pgsr, PgStreamingReadDevLog *log)
{
	pgsr->dev_log = log;
}

void
pg_streaming_read_free_dev_log(PgStreamingRead *pgsr)
{
	Assert(pgsr->dev_log);
	pfree(pgsr->dev_log->completion_log);
	pfree(pgsr->dev_log->consumption_log);
	pfree(pgsr->dev_log->wait_log);
	pfree(pgsr->dev_log);
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

	if (pgsr->consumptions != NULL)
		pfree(pgsr->consumptions);

	pfree(pgsr);
}

void
pgsr_add_consumption(PgStreamingReadConsumptionRing *ring, instr_time latency,
		int prefetch_distance)
{
	PgStreamingReadConsumption *target;

	if (ring->idx == 0)
		ring->idx = PGSR_RING_SIZE;

	ring->idx--;

	target = &ring->data[ring->idx];

	target->latency = latency;
	target->prefetch_distance = prefetch_distance;

	if (ring->num_valid < PGSR_RING_SIZE)
		ring->num_valid++;
}

static void
pg_streaming_read_complete(PgAioOnCompletionLocalContext *ocb, PgAioInProgress *io)
{
	PgStreamingReadItem *this_read = pgaio_ocb_container(PgStreamingReadItem, on_completion, ocb);
	PgStreamingRead *pgsr = this_read->pgsr;

	/*
	 * io->completed should ideally be set by this point.
	 * pgaio_process_io_completion() sets it. TODO: eventually, ensure it is
	 * set as close to completion as possible and stop checking here.
	 * what about pgaio_complete_ios()?
	 */
	if (INSTR_TIME_IS_ZERO(io->completed))
		INSTR_TIME_SET_CURRENT(io->completed);

	if (pgsr->dev_log)
		aio_dev_log_completion(pgsr->dev_log->completion_log, io->submitted, io->completed,
				this_read->prefetch_distance, pgsr->completed_count, pgsr->inflight_count);

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
	this_read->latency = this_read->aio->completed;
	INSTR_TIME_SUBTRACT(this_read->latency, this_read->aio->submitted);
	pgaio_io_recycle(this_read->aio);

	pg_streaming_read_prefetch(pgsr);
}

float pgsr_avg_tput(PgStreamingReadConsumptionRing *ring)
{
	float avg_tput = 0;

	for (int i = 0; i < PGSR_RING_SIZE && i < ring->num_valid; i++)
	{
		PgStreamingReadConsumption *cur = &ring->data[i];

		avg_tput += (cur->prefetch_distance / INSTR_TIME_GET_MILLISEC(cur->latency));
	}

	return avg_tput / ring->num_valid;
}

int pgsr_max_pfd(PgStreamingReadConsumptionRing *ring)
{
	int max_pfd = 0;

	for (int i = 0; i < PGSR_RING_SIZE && i < ring->num_valid; i++)
	{
		int pfd = ring->data[i].prefetch_distance;
		if (pfd > max_pfd)
			max_pfd = pfd;
	}

	return max_pfd;
}

float pgsr_avg_pfd(PgStreamingReadConsumptionRing *ring)
{
	float avg_pfd = 0;

	for (int i = 0; i < PGSR_RING_SIZE && i < ring->num_valid; i++)
		avg_pfd += ring->data[i].prefetch_distance;

	return avg_pfd / ring->num_valid;
}


static void
pg_streaming_read_prefetch_one(PgStreamingRead *pgsr)
{
	PgStreamingReadItem *this_read;
	PgStreamingReadNextStatus status;

	Assert(!dlist_is_empty(&pgsr->available));

	this_read = dlist_container(PgStreamingReadItem, node, dlist_pop_head_node(&pgsr->available));
	Assert(!this_read->valid);
	Assert(!this_read->in_progress);
	Assert(this_read->read_private == 0);
	INSTR_TIME_SET_ZERO(this_read->latency);
	this_read->prefetch_distance = pgsr->prefetch_distance;

	if (this_read->aio == NULL)
	{
		this_read->aio = pgaio_io_get();
	}

	pgaio_io_on_completion_local(this_read->aio, &this_read->on_completion);
	this_read->in_progress = true;
	this_read->valid = true;
	this_read->real_io = false;
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
		INSTR_TIME_SET_ZERO(this_read->consumed);
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
		Assert(this_read->read_private != 0);
		this_read->real_io = true;
	}
}

static void
pg_streaming_read_prefetch(PgStreamingRead *pgsr)
{
	uint32 min_issue;

	if (pgsr->hit_end)
		return;

	Assert(pgsr->inflight_count <= pgsr->current_window);
	Assert(pgsr->completed_count <= (pgsr->iodepth_max + pgsr->distance_max));

	/*
	 * XXX: Some issues:
	 *
	 * - We should probably do the window calculation based on the number of
	 *   buffers the user actually requested, i.e. only recompute this
	 *   whenever pg_streaming_read_get_next() is called. Otherwise we will
	 *   always read the whole prefetch window.
	 *
	 * - The algorithm here is pretty stupid. Should take distance / iodepth
	 *   properly into account in a distitcn way. Grow slower.
	 *
	 * - It'd be good to have a usage dependent iodepth management. After an
	 *   initial increase, we should only increase further if the the user the
	 *   window proves too small (i.e. we don't manage to keep 'completed'
	 *   close to full).
	 *
	 * - If most requests don't trigger IO, we should probably reduce the
	 *   prefetch window.
	 */
	if (pgsr->current_window < pgsr->iodepth_max)
	{
		if (pgsr->current_window == 0)
			pgsr->current_window = 4;
		else
			pgsr->current_window *= 2;

		if (pgsr->current_window > pgsr->iodepth_max)
			pgsr->current_window = pgsr->iodepth_max;

		min_issue = 1;
	}
	else
	{
		min_issue = Min(pgsr->iodepth_max, pgsr->current_window / 4);
	}

	Assert(pgsr->inflight_count <= pgsr->current_window);
	Assert(pgsr->completed_count <= (pgsr->iodepth_max + pgsr->distance_max));

	if (pgsr->completed_count >= pgsr->current_window)
		return;

	if (pgsr->inflight_count >= pgsr->current_window)
		return;

	while (!pgsr->hit_end &&
		   (pgsr->inflight_count < pgsr->current_window) &&
		   (pgsr->completed_count < pgsr->current_window))
	{
		pg_streaming_read_prefetch_one(pgsr);
		pgaio_limit_pending(false, min_issue);

		CHECK_FOR_INTERRUPTS();
	}
}

uintptr_t
pg_streaming_read_get_next(PgStreamingRead *pgsr)
{
	if (pgsr->prefetched_total_count == 0)
	{
		pg_streaming_read_prefetch(pgsr);
		Assert(pgsr->hit_end || pgsr->prefetched_total_count > 0);
	}

	if (dlist_is_empty(&pgsr->in_order))
	{
		Assert(pgsr->hit_end);

		if (pgsr->dev_log)
			aio_dev_write_log(pgsr->dev_log, pgsr->iodepth_max + pgsr->distance_max);

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

		if (this_read->in_progress)
		{
			instr_time wait_start, wait_end;

			INSTR_TIME_SET_CURRENT(wait_start);

			pgaio_io_wait(this_read->aio);

			INSTR_TIME_SET_CURRENT(wait_end);

			if (pgsr->dev_log)
				aio_dev_log_wait(pgsr->dev_log->wait_log, wait_start, wait_end);

			/* callback should have updated */
			Assert(!this_read->in_progress);
			Assert(this_read->valid);
		}

		if (this_read->real_io)
		{
			INSTR_TIME_SET_CURRENT(this_read->consumed);

			if (pgsr->dev_log)
				aio_dev_log_consumption(pgsr->dev_log->consumption_log, this_read->consumed);

			if (pgsr->consumptions)
			{
				// TODO: don't know what belongs here and what belongs in
				// pg_streaming_read_complete()
				pgsr_add_consumption(pgsr->consumptions, this_read->latency, this_read->prefetch_distance);
			}
		}

		Assert(this_read->read_private != 0);
		ret = this_read->read_private;
		this_read->read_private = 0;
		this_read->valid = false;

		pgsr->completed_count--;
		INSTR_TIME_SET_ZERO(this_read->consumed);
		dlist_push_tail(&pgsr->available, &this_read->node);
		pg_streaming_read_prefetch(pgsr);

		return ret;
	}
}
