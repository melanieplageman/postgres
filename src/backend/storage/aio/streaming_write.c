/*-------------------------------------------------------------------------
 *
 * streaming_write.c
 *	  Asynchronous I/O subsytem - helpers for using AIO
 *
 * Portions Copyright (c) 1996-2021, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 * IDENTIFICATION
 *	  src/backend/storage/aio/streaming_write.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include "miscadmin.h"
#include "storage/aio_internal.h"
#include "storage/bufmgr.h"
#include "storage/streaming_write.h"


/* typedef is in header */
typedef struct PgStreamingWriteItem
{
	/* membership in PgStreamingWrite->issued, PgStreamingWrite->available */
	dlist_node	node;

	PgAioInProgress *aio;
	bool		in_purgatory;
	bool		in_progress;
	void	   *private_data;

	struct PgStreamingWrite *pgsw;

	PgStreamingWriteCompleted on_completion_user;
	PgStreamingWriteRetry on_failure_user;

	PgAioOnCompletionLocalContext on_completion_aio;
} PgStreamingWriteItem;

/* typedef is in header */
struct PgStreamingWrite
{
	uint32		iodepth;

	uint32		inflight_count;

	void	   *private_data;

	/* submitted writes */
	dlist_head	issued;

	/* available writes (unused or completed) */
	dlist_head	available;

	uint32		max_since_submit;

	/*
	 * IOs returned by pg_streaming_write_get_io, before
	 * pg_streaming_write_release_io or pg_streaming_write_write has been
	 * called.
	 */
	dlist_head	purgatory;

	PgStreamingWriteItem all_items[FLEXIBLE_ARRAY_MEMBER];
};


static void pg_streaming_write_complete(PgAioOnCompletionLocalContext *ocb, PgAioInProgress *io);


PgStreamingWrite *
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
	dlist_iter	iter;
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
	void	   *private_data = this_write->private_data;
	int			result;

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
