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

#include "miscadmin.h"
#include "storage/streaming_read.h"
#include "storage/aio_internal.h"
#include "storage/bufmgr.h"


typedef struct PgStreamingReadItem
{
	/* membership in PgStreamingRead->issued, PgStreamingRead->available */
	dlist_node	node;
	/* membership in PgStreamingRead->in_order */
	dlist_node	sequence_node;

	PgAioOnCompletionLocalContext on_completion;
	PgStreamingRead *pgsr;
	PgAioInProgress *aio;

	/* is this IO still considered to be in progress */
	bool		in_progress;
	/* is this item currently valid / used */
	bool		valid;
	void	   *io_private;
} PgStreamingReadItem;

typedef void (*PgStreamingReadLayerFreeCB) (PgStreamingRead *pgsr);

struct PgStreamingRead
{
	uint32		iodepth_max;
	uint32		distance_max;
	uint32		all_items_count;

	uint32		per_io_private_size;

	uintptr_t	pgsr_private;
	PgStreamingReadDetermineNextCB determine_next_cb;
	PgStreamingReadRelease release_cb;

	/*
	 * Layers ontop of a "base" pgsr may need to free resources first.
	 */
	uintptr_t	layer_private;
	PgStreamingReadLayerFreeCB layer_free_cb;

	uint32		current_window;

	/* number of requests issued */
	uint64		prefetched_total_count;
	/* number of requests submitted to kernel */
	uint64		submitted_total_count;
	/* number of current requests completed */
	uint32		completed_count;
	/* number of current requests in flight */
	int32		inflight_count;
	/* number of requests that didn't require IO (debugging only) */
	int32		no_io_count;

	bool		hit_end;

	/* submitted reads */
	dlist_head	issued;

	/* available reads (unused or completed) */
	dlist_head	available;

	/*
	 * Last item returned by pg_streaming_read_get_next() et al. Not yet added
	 * to ->available, as last_returned->io_private might still be used by
	 * caller.
	 */
	PgStreamingReadItem *last_returned;

	/*
	 * IOs, be they completed or in progress, in the order that the callback
	 * returned them.
	 */
	dlist_head	in_order;

	PgStreamingReadItem all_items[FLEXIBLE_ARRAY_MEMBER];
};

static void pg_streaming_read_complete(PgAioOnCompletionLocalContext *ocb, PgAioInProgress *io);
static void pg_streaming_read_prefetch(PgStreamingRead *pgsr);

/*
 * Allocates a streaming read instances.
 *
 * Each IO has per_io_private_size private memory, which can be set in
 * determine_next_cb() and returned pg_streaming_read_get_next().
 */
PgStreamingRead *
pg_streaming_read_alloc(uint32 iodepth,
						uint32 per_io_private_size, uintptr_t pgsr_private,
						PgStreamingReadDetermineNextCB determine_next_cb,
						PgStreamingReadRelease release_cb)
{
	PgStreamingRead *pgsr;
	uint32		all_items_count;
	size_t		pgsr_sz, total_sz;
	char	   *p;

	iodepth = Max(Min(iodepth, NBuffers / 128), 1);
	all_items_count = iodepth * 2;

	pgsr_sz = offsetof(PgStreamingRead, all_items) +
		all_items_count * sizeof(PgStreamingReadItem);
	total_sz = pgsr_sz +
		all_items_count * per_io_private_size;

	p = palloc0(total_sz);
	pgsr = (PgStreamingRead *) p;
	p += pgsr_sz;

	pgsr->iodepth_max = iodepth;
	pgsr->distance_max = iodepth;
	pgsr->all_items_count = all_items_count;
	pgsr->pgsr_private = pgsr_private;
	pgsr->determine_next_cb = determine_next_cb;
	pgsr->release_cb = release_cb;

	pgsr->current_window = 0;

	dlist_init(&pgsr->available);
	dlist_init(&pgsr->in_order);
	dlist_init(&pgsr->issued);

	for (int i = 0; i < pgsr->all_items_count; i++)
	{
		PgStreamingReadItem *this_read = &pgsr->all_items[i];

		this_read->on_completion.callback = pg_streaming_read_complete;
		this_read->pgsr = pgsr;
		this_read->io_private = p;
		p += per_io_private_size;

		dlist_push_tail(&pgsr->available, &this_read->node);
	}

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
			pgsr->release_cb(pgsr->pgsr_private, this_read->io_private);

		if (this_read->aio)
		{
			pgaio_io_release(this_read->aio);
			this_read->aio = NULL;
		}
	}

	/*
	 * Give layers ontop of the base pgsr to free resources themselvs.
	 */
	if (pgsr->layer_free_cb)
		pgsr->layer_free_cb(pgsr);

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

static void
pg_streaming_read_prefetch_one(PgStreamingRead *pgsr)
{
	PgStreamingReadItem *this_read;
	PgStreamingReadNextStatus status;

	Assert(!dlist_is_empty(&pgsr->available));

	this_read = dlist_container(PgStreamingReadItem, node, dlist_pop_head_node(&pgsr->available));
	Assert(!this_read->valid);
	Assert(!this_read->in_progress);

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

	status = pgsr->determine_next_cb(pgsr, pgsr->pgsr_private,
									 this_read->aio,
									 this_read->io_private);

	if (status == PGSR_NEXT_END)
	{
		pgsr->inflight_count--;
		pgsr->prefetched_total_count--;
		pgsr->hit_end = true;
		/* FIXME: assert only */
		memset(this_read->io_private, 0x3f, pgsr->per_io_private_size);
		this_read->valid = false;
		this_read->in_progress = false;
		pgaio_io_recycle(this_read->aio);
		dlist_delete_from(&pgsr->in_order, &this_read->sequence_node);
		dlist_push_tail(&pgsr->available, &this_read->node);
	}
	else if (status == PGSR_NEXT_NO_IO)
	{
		pgsr->inflight_count--;
		pgsr->no_io_count++;
		pgsr->completed_count++;
		this_read->in_progress = false;
		pgaio_io_recycle(this_read->aio);
		dlist_delete_from(&pgsr->issued, &this_read->node);
	}
}

static void
pg_streaming_read_prefetch(PgStreamingRead *pgsr)
{
	uint32		min_issue;

	if (pgsr->hit_end)
		return;

	Assert(pgsr->inflight_count <= pgsr->current_window);
	Assert(pgsr->completed_count <= (pgsr->iodepth_max + pgsr->distance_max));

	/*
	 * XXX: Some issues:
	 *
	 * - We should probably do the window calculation based on the number of
	 * buffers the user actually requested, i.e. only recompute this whenever
	 * pg_streaming_read_get_next() is called. Otherwise we will always read
	 * the whole prefetch window.
	 *
	 * - The algorithm here is pretty stupid. Should take distance / iodepth
	 * properly into account in a distitcn way. Grow slower.
	 *
	 * - It'd be good to have a usage dependent iodepth management. After an
	 * initial increase, we should only increase further if the the user the
	 * window proves too small (i.e. we don't manage to keep 'completed' close
	 * to full).
	 *
	 * - If most requests don't trigger IO, we should probably reduce the
	 * prefetch window.
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

void *
pg_streaming_read_get_next(PgStreamingRead *pgsr)
{
	if (pgsr->last_returned)
	{
		dlist_push_tail(&pgsr->available, &pgsr->last_returned->node);
		pgsr->last_returned = NULL;
	}

	if (pgsr->prefetched_total_count == 0)
	{
		pg_streaming_read_prefetch(pgsr);
		Assert(pgsr->hit_end || pgsr->prefetched_total_count > 0);
	}

	if (dlist_is_empty(&pgsr->in_order))
	{
		Assert(pgsr->hit_end);
		return 0;
	}
	else
	{
		PgStreamingReadItem *this_read;
		void	*ret;

		Assert(pgsr->prefetched_total_count > 0);

		this_read = dlist_container(PgStreamingReadItem, sequence_node,
									dlist_pop_head_node(&pgsr->in_order));
		Assert(this_read->valid);

		if (this_read->in_progress)
		{
			pgaio_io_wait(this_read->aio);
			/* callback should have updated */
			Assert(!this_read->in_progress);
			Assert(this_read->valid);
		}

		/*
		 * Queue this_read to be reused during the next call to
		 * pg_streaming_read_get_next(). This is deferred, so that the caller
		 * can use io_private to stash data without needing per-io memory
		 * allocations.
		 */
		pgsr->last_returned = this_read;

		ret = this_read->io_private;
		this_read->valid = false;

		pgsr->completed_count--;
		pg_streaming_read_prefetch(pgsr);

		return ret;
	}
}


typedef struct PgStreamingReadBufferPrivate
{
	PgStreamingReadBufferDetermineNextCB next_cb;
	BufferAccessStrategy strategy;
} PgStreamingReadBufferPrivate;

typedef struct PgStreamingReadBufferIOPrivate
{
	Buffer		buffer;
	char		buffer_io_private[FLEXIBLE_ARRAY_MEMBER];
} PgStreamingReadBufferIOPrivate;

static void
pg_streaming_read_buffer_release(uintptr_t pgsr_private, void *read_private)
{
	PgStreamingReadBufferIOPrivate *buf_io_priv = read_private;
	Buffer		buf = buf_io_priv->buffer;

	Assert(BufferIsValid(buf));
	ReleaseBuffer(buf);
}

static PgStreamingReadNextStatus
pg_streaming_read_buffer_next(PgStreamingRead *pgsr, uintptr_t pgsr_private,
							  PgAioInProgress *aio, void *read_private)
{
	PgStreamingReadBufferPrivate *pgsr_buf = (PgStreamingReadBufferPrivate *) pgsr->layer_private;
	PgStreamingReadBufferIOPrivate *buf_io_priv = read_private;
	Relation	rel;
	ForkNumber	fork;
	BlockNumber blocknum;
	ReadBufferMode mode;
	bool		already_valid;

	blocknum = pgsr_buf->next_cb(pgsr, pgsr_private, &buf_io_priv->buffer_io_private, &rel, &fork, &mode);

	if (blocknum == InvalidBlockNumber)
		return PGSR_NEXT_END;

	buf_io_priv->buffer =
		ReadBufferAsync(rel, fork, blocknum, mode, pgsr_buf->strategy,
						&already_valid, &aio);

	if (already_valid)
		return PGSR_NEXT_NO_IO;
	else
		return PGSR_NEXT_IO;
}

static void
pg_streaming_read_buffer_free(PgStreamingRead *pgsr)
{
	PgStreamingReadBufferPrivate *pgsr_buf = (PgStreamingReadBufferPrivate *) pgsr->layer_private;

	Assert(pgsr_buf != NULL);
	pfree(pgsr_buf);
	pgsr->layer_private = 0;
}

PgStreamingRead *
pg_streaming_read_buffer_alloc(uint32 iodepth,
							   uint32 per_io_private_size, uintptr_t pgsr_private,
							   BufferAccessStrategy strategy,
							   PgStreamingReadBufferDetermineNextCB determine_next_cb)
{
	PgStreamingReadBufferPrivate *pgsr_buf;
	PgStreamingRead *pgsr;

	pgsr_buf = palloc0(sizeof(PgStreamingReadBufferPrivate));
	pgsr_buf->next_cb = determine_next_cb;
	pgsr_buf->strategy = strategy;

	/*
	 * We stash extra state in the per-io state. This is largely invisible to
	 * the caller, because we pass an offset into that allocation to
	 * dtermine_next_cb() / return it in pg_streaming_read_buffer_get_next().
	 */
	pgsr = pg_streaming_read_alloc(iodepth,
								   per_io_private_size +
								   offsetof(PgStreamingReadBufferIOPrivate, buffer_io_private),
								   pgsr_private,
								   pg_streaming_read_buffer_next,
								   pg_streaming_read_buffer_release);

	pgsr->layer_private = (uintptr_t) pgsr_buf;
	pgsr->layer_free_cb = pg_streaming_read_buffer_free;

	return pgsr;
}

Buffer
pg_streaming_read_buffer_get_next(PgStreamingRead *pgsr, void **io_private)
{
	PgStreamingReadBufferIOPrivate *buf_io_priv;

	buf_io_priv = pg_streaming_read_get_next(pgsr);

	if (buf_io_priv == NULL)
		return InvalidBuffer;

	if (io_private)
		*io_private = &buf_io_priv->buffer_io_private;

	return buf_io_priv->buffer;
}
