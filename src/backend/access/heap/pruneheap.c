/*-------------------------------------------------------------------------
 *
 * pruneheap.c
 *	  heap page pruning and HOT-chain management code
 *
 * Portions Copyright (c) 1996-2024, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/backend/access/heap/pruneheap.c
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "access/heapam.h"
#include "access/heapam_xlog.h"
#include "access/htup_details.h"
#include "access/transam.h"
#include "access/xlog.h"
#include "access/xloginsert.h"
#include "catalog/catalog.h"
#include "executor/instrument.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "storage/bufmgr.h"
#include "utils/snapmgr.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

/* Working data for heap_page_prune and subroutines */
typedef struct
{
	Relation	rel;

	/* tuple visibility test, initialized for the relation */
	GlobalVisState *vistest;
	/* whether or not dead items can be set LP_UNUSED during pruning */
	bool		mark_unused_now;

	TransactionId new_prune_xid;	/* new prune hint value for page */
	TransactionId snapshotConflictHorizon;	/* latest xid removed */
	int			nredirected;	/* numbers of entries in arrays below */
	int			ndead;
	int			nunused;
	/* arrays that accumulate indexes of items to be changed */
	OffsetNumber redirected[MaxHeapTuplesPerPage * 2];
	OffsetNumber nowdead[MaxHeapTuplesPerPage];
	OffsetNumber nowunused[MaxHeapTuplesPerPage];

	/*
	 * marked[i] is true if item i is entered in one of the above arrays.
	 *
	 * This needs to be MaxHeapTuplesPerPage + 1 long as FirstOffsetNumber is
	 * 1. Otherwise every access would need to subtract 1.
	 */
	bool		marked[MaxHeapTuplesPerPage + 1];

	/*
	 * Tuple visibility is only computed once for each tuple, for correctness
	 * and efficiency reasons; see comment in heap_page_prune() for details.
	 * This is of type int8[], instead of HTSV_Result[], so we can use -1 to
	 * indicate no visibility has been computed, e.g. for LP_DEAD items.
	 *
	 * This needs to be MaxHeapTuplesPerPage + 1 long as FirstOffsetNumber is
	 * 1. Otherwise every access would need to subtract 1.
	 */
	int8		htsv[MaxHeapTuplesPerPage + 1];
} PruneState;

/* Local functions */
static HTSV_Result heap_prune_satisfies_vacuum(PruneState *prstate,
											   HeapTuple tup,
											   Buffer buffer);
static int	heap_prune_chain(Buffer buffer,
							 OffsetNumber rootoffnum,
							 PruneState *prstate, PruneResult *presult);

static inline HTSV_Result htsv_get_valid_status(int status);
static void prune_prepare_freeze_tuple(Page page, OffsetNumber offnum, PruneState *prstate,
									   HeapPageFreeze *pagefrz, HeapTupleFreeze *frozen,
									   PruneResult *presult);
static void heap_prune_record_prunable(PruneState *prstate, TransactionId xid);
static void heap_prune_record_redirect(PruneState *prstate,
									   OffsetNumber offnum, OffsetNumber rdoffnum,
									   PruneResult *presult);
static void heap_prune_record_dead(PruneState *prstate, OffsetNumber offnum,
								   PruneResult *presult);
static void heap_prune_record_dead_or_unused(PruneState *prstate, OffsetNumber offnum,
											 PruneResult *presult);
static void heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum);
static void page_verify_redirects(Page page);


/*
 * Optionally prune and repair fragmentation in the specified page.
 *
 * This is an opportunistic function.  It will perform housekeeping
 * only if the page heuristically looks like a candidate for pruning and we
 * can acquire buffer cleanup lock without blocking.
 *
 * Note: this is called quite often.  It's important that it fall out quickly
 * if there's not any use in pruning.
 *
 * Caller must have pin on the buffer, and must *not* have a lock on it.
 */
void
heap_page_prune_opt(Relation relation, Buffer buffer)
{
	Page		page = BufferGetPage(buffer);
	TransactionId prune_xid;
	GlobalVisState *vistest;
	Size		minfree;

	/*
	 * We can't write WAL in recovery mode, so there's no point trying to
	 * clean the page. The primary will likely issue a cleaning WAL record
	 * soon anyway, so this is no particular loss.
	 */
	if (RecoveryInProgress())
		return;

	/*
	 * First check whether there's any chance there's something to prune,
	 * determining the appropriate horizon is a waste if there's no prune_xid
	 * (i.e. no updates/deletes left potentially dead tuples around).
	 */
	prune_xid = ((PageHeader) page)->pd_prune_xid;
	if (!TransactionIdIsValid(prune_xid))
		return;

	/*
	 * Check whether prune_xid indicates that there may be dead rows that can
	 * be cleaned up.
	 */
	vistest = GlobalVisTestFor(relation);

	if (!GlobalVisTestIsRemovableXid(vistest, prune_xid))
		return;

	/*
	 * We prune when a previous UPDATE failed to find enough space on the page
	 * for a new tuple version, or when free space falls below the relation's
	 * fill-factor target (but not less than 10%).
	 *
	 * Checking free space here is questionable since we aren't holding any
	 * lock on the buffer; in the worst case we could get a bogus answer. It's
	 * unlikely to be *seriously* wrong, though, since reading either pd_lower
	 * or pd_upper is probably atomic.  Avoiding taking a lock seems more
	 * important than sometimes getting a wrong answer in what is after all
	 * just a heuristic estimate.
	 */
	minfree = RelationGetTargetPageFreeSpace(relation,
											 HEAP_DEFAULT_FILLFACTOR);
	minfree = Max(minfree, BLCKSZ / 10);

	if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
	{
		/* OK, try to get exclusive buffer lock */
		if (!ConditionalLockBufferForCleanup(buffer))
			return;

		/*
		 * Now that we have buffer lock, get accurate information about the
		 * page's free space, and recheck the heuristic about whether to
		 * prune.
		 */
		if (PageIsFull(page) || PageGetHeapFreeSpace(page) < minfree)
		{
			PruneResult presult;

			/*
			 * For now, pass mark_unused_now as false regardless of whether or
			 * not the relation has indexes, since we cannot safely determine
			 * that during on-access pruning with the current implementation.
			 */
			heap_page_prune(relation, buffer, vistest, false, NULL,
							&presult, NULL);

			/*
			 * Report the number of tuples reclaimed to pgstats.  This is
			 * presult.ndeleted minus the number of newly-LP_DEAD-set items.
			 *
			 * We derive the number of dead tuples like this to avoid totally
			 * forgetting about items that were set to LP_DEAD, since they
			 * still need to be cleaned up by VACUUM.  We only want to count
			 * heap-only tuples that just became LP_UNUSED in our report,
			 * which don't.
			 *
			 * VACUUM doesn't have to compensate in the same way when it
			 * tracks ndeleted, since it will set the same LP_DEAD items to
			 * LP_UNUSED separately.
			 */
			if (presult.ndeleted > presult.nnewlpdead)
				pgstat_update_heap_dead_tuples(relation,
											   presult.ndeleted - presult.nnewlpdead);
		}

		/* And release buffer lock */
		LockBuffer(buffer, BUFFER_LOCK_UNLOCK);

		/*
		 * We avoid reuse of any free space created on the page by unrelated
		 * UPDATEs/INSERTs by opting to not update the FSM at this point.  The
		 * free space should be reused by UPDATEs to *this* page.
		 */
	}
}


/*
 * Prune and repair fragmentation in the specified page.
 *
 * Caller must have pin and buffer cleanup lock on the page.  Note that we
 * don't update the FSM information for page on caller's behalf.  Caller might
 * also need to account for a reduction in the length of the line pointer
 * array following array truncation by us.
 *
 * vistest is used to distinguish whether tuples are DEAD or RECENTLY_DEAD
 * (see heap_prune_satisfies_vacuum and
 * HeapTupleSatisfiesVacuum).
 *
 * mark_unused_now indicates whether or not dead items can be set LP_UNUSED during
 * pruning.
 *
 * pagefrz contains both input and output parameters used if the caller is
 * interested in potentially freezing tuples on the page.
 *
 * off_loc is the offset location required by the caller to use in error
 * callback.
 *
 * presult contains output parameters needed by callers such as the number of
 * tuples removed and the number of line pointers newly marked LP_DEAD.
 * heap_page_prune() is responsible for initializing it.
 */
void
heap_page_prune(Relation relation, Buffer buffer,
				GlobalVisState *vistest,
				bool mark_unused_now,
				HeapPageFreeze *pagefrz,
				PruneResult *presult,
				OffsetNumber *off_loc)
{
	Page		page = BufferGetPage(buffer);
	BlockNumber blockno = BufferGetBlockNumber(buffer);
	OffsetNumber offnum,
				maxoff;
	PruneState	prstate;
	HeapTupleData tup;
	bool		do_freeze;
	bool		do_prune;
	bool		whole_page_freezable;
	bool		hint_bit_fpi;
	bool		prune_fpi = false;
	int64		fpi_before = pgWalUsage.wal_fpi;
	TransactionId frz_conflict_horizon = InvalidTransactionId;

	/*
	 * One entry for every tuple that we may freeze.
	 */
	HeapTupleFreeze frozen[MaxHeapTuplesPerPage];

	/*
	 * Our strategy is to scan the page and make lists of items to change,
	 * then apply the changes within a critical section.  This keeps as much
	 * logic as possible out of the critical section, and also ensures that
	 * WAL replay will work the same as the normal case.
	 *
	 * First, initialize the new pd_prune_xid value to zero (indicating no
	 * prunable tuples).  If we find any tuples which may soon become
	 * prunable, we will save the lowest relevant XID in new_prune_xid. Also
	 * initialize the rest of our working state.
	 */
	prstate.new_prune_xid = InvalidTransactionId;
	prstate.rel = relation;
	prstate.vistest = vistest;
	prstate.mark_unused_now = mark_unused_now;
	prstate.snapshotConflictHorizon = InvalidTransactionId;
	prstate.nredirected = prstate.ndead = prstate.nunused = 0;
	memset(prstate.marked, 0, sizeof(prstate.marked));

	/*
	 * presult->htsv is not initialized here because all ntuple spots in the
	 * array will be set either to a valid HTSV_Result value or -1.
	 */
	presult->ndeleted = 0;
	presult->nnewlpdead = 0;
	presult->nfrozen = 0;

	presult->hastup = false;

	presult->live_tuples = 0;
	presult->recently_dead_tuples = 0;

	/*
	 * Keep track of whether or not the page is all_visible in case the caller
	 * wants to use this information to update the VM.
	 */
	presult->all_visible = true;
	/* for recovery conflicts */
	presult->frz_conflict_horizon = InvalidTransactionId;

	maxoff = PageGetMaxOffsetNumber(page);
	tup.t_tableOid = RelationGetRelid(prstate.rel);

	/*
	 * Determine HTSV for all tuples.
	 *
	 * This is required for correctness to deal with cases where running HTSV
	 * twice could result in different results (e.g. RECENTLY_DEAD can turn to
	 * DEAD if another checked item causes GlobalVisTestIsRemovableFullXid()
	 * to update the horizon, INSERT_IN_PROGRESS can change to DEAD if the
	 * inserting transaction aborts, ...). That in turn could cause
	 * heap_prune_chain() to behave incorrectly if a tuple is reached twice,
	 * once directly via a heap_prune_chain() and once following a HOT chain.
	 *
	 * It's also good for performance. Most commonly tuples within a page are
	 * stored at decreasing offsets (while the items are stored at increasing
	 * offsets). When processing all tuples on a page this leads to reading
	 * memory at decreasing offsets within a page, with a variable stride.
	 * That's hard for CPU prefetchers to deal with. Processing the items in
	 * reverse order (and thus the tuples in increasing order) increases
	 * prefetching efficiency significantly / decreases the number of cache
	 * misses.
	 */
	for (offnum = maxoff;
		 offnum >= FirstOffsetNumber;
		 offnum = OffsetNumberPrev(offnum))
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		HeapTupleHeader htup;

		/* Nothing to do if slot doesn't contain a tuple */
		if (!ItemIdIsNormal(itemid))
		{
			prstate.htsv[offnum] = -1;
			continue;
		}

		htup = (HeapTupleHeader) PageGetItem(page, itemid);
		tup.t_data = htup;
		tup.t_len = ItemIdGetLength(itemid);
		ItemPointerSet(&(tup.t_self), blockno, offnum);

		/*
		 * Set the offset number so that we can display it along with any
		 * error that occurred while processing this tuple.
		 */
		if (off_loc)
			*off_loc = offnum;

		prstate.htsv[offnum] = heap_prune_satisfies_vacuum(&prstate, &tup,
														   buffer);
		Assert(ItemIdIsNormal(itemid));

		/*
		 * The criteria for counting a tuple as live in this block need to
		 * match what analyze.c's acquire_sample_rows() does, otherwise VACUUM
		 * and ANALYZE may produce wildly different reltuples values, e.g.
		 * when there are many recently-dead tuples.
		 *
		 * The logic here is a bit simpler than acquire_sample_rows(), as
		 * VACUUM can't run inside a transaction block, which makes some cases
		 * impossible (e.g. in-progress insert from the same transaction).
		 *
		 * We treat LP_DEAD items (which are the closest thing to DEAD tuples
		 * that might be seen here) differently, too: we assume that they'll
		 * become LP_UNUSED before VACUUM finishes.  This difference is only
		 * superficial.  VACUUM effectively agrees with ANALYZE about DEAD
		 * items, in the end.  VACUUM won't remember LP_DEAD items, but only
		 * because they're not supposed to be left behind when it is done.
		 * (Cases where we bypass index vacuuming will violate this optimistic
		 * assumption, but the overall impact of that should be negligible.)
		 */
		switch (prstate.htsv[offnum])
		{
			case HEAPTUPLE_DEAD:

				/*
				 * Deliberately delay unsetting all_visible until later during
				 * pruning. Removable dead tuples shouldn't preclude freezing
				 * the page. After finishing this first pass of tuple
				 * visibility checks, initialize all_visible_except_removable
				 * with the current value of all_visible to indicate whether
				 * or not the page is all visible except for dead tuples. This
				 * will allow us to attempt to freeze the page after pruning.
				 * Later during pruning, if we encounter an LP_DEAD item or
				 * are setting an item LP_DEAD, we will unset all_visible. As
				 * long as we unset it before updating the visibility map,
				 * this will be correct.
				 */
				break;
			case HEAPTUPLE_LIVE:

				/*
				 * Count it as live.  Not only is this natural, but it's also
				 * what acquire_sample_rows() does.
				 */
				presult->live_tuples++;

				/*
				 * Is the tuple definitely visible to all transactions?
				 *
				 * NB: Like with per-tuple hint bits, we can't set the
				 * PD_ALL_VISIBLE flag if the inserter committed
				 * asynchronously. See SetHintBits for more info. Check that
				 * the tuple is hinted xmin-committed because of that.
				 */
				if (presult->all_visible)
				{
					TransactionId xmin;

					if (!HeapTupleHeaderXminCommitted(htup))
					{
						presult->all_visible = false;
						break;
					}

					/*
					 * The inserter definitely committed. But is it old enough
					 * that everyone sees it as committed?
					 */
					xmin = HeapTupleHeaderGetXmin(htup);
					if (!GlobalVisTestIsRemovableXid(vistest, xmin))
					{
						presult->all_visible = false;
						break;
					}

					/* Track newest xmin on page. */
					if (TransactionIdFollows(xmin, presult->frz_conflict_horizon) &&
						TransactionIdIsNormal(xmin))
						presult->frz_conflict_horizon = xmin;
				}
				break;
			case HEAPTUPLE_RECENTLY_DEAD:

				/*
				 * If tuple is recently dead then we must not remove it from
				 * the relation.  (We only remove items that are LP_DEAD from
				 * pruning.)
				 */
				presult->recently_dead_tuples++;
				presult->all_visible = false;
				break;
			case HEAPTUPLE_INSERT_IN_PROGRESS:

				/*
				 * We do not count these rows as live, because we expect the
				 * inserting transaction to update the counters at commit, and
				 * we assume that will happen only after we report our
				 * results.  This assumption is a bit shaky, but it is what
				 * acquire_sample_rows() does, so be consistent.
				 */
				presult->all_visible = false;
				break;
			case HEAPTUPLE_DELETE_IN_PROGRESS:

				/*
				 * This an expected case during concurrent vacuum. Count such
				 * rows as live.  As above, we assume the deleting transaction
				 * will commit and update the counters after we report.
				 */
				presult->live_tuples++;
				presult->all_visible = false;
				break;
			default:
				elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
				break;
		}
	}

	/*
	 * If checksums are enabled, heap_prune_satisfies_vacuum() may have caused
	 * an FPI to be emitted. Then reset fpi_before for no prune case.
	 */
	hint_bit_fpi = fpi_before != pgWalUsage.wal_fpi;
	fpi_before = pgWalUsage.wal_fpi;

	/*
	 * For vacuum, if the whole page will become frozen, we consider
	 * opportunistically freezing tuples. Dead tuples which will be removed by
	 * the end of vacuuming should not preclude us from opportunistically
	 * freezing. We will not be able to freeze the whole page if there are
	 * tuples present which are not visible to everyone or if there are dead
	 * tuples which are not yet removable. We need all_visible to be false if
	 * LP_DEAD tuples remain after pruning so that we do not incorrectly
	 * update the visibility map or page hint bit. So, we will update
	 * presult->all_visible to reflect the presence of LP_DEAD items while
	 * pruning and keep all_visible_except_removable to permit freezing if the
	 * whole page will eventually become all visible after removing tuples.
	 */
	presult->all_visible_except_removable = presult->all_visible;

	/*
	 * We will update the VM after pruning, collecting LP_DEAD items, and
	 * freezing tuples. Keep track of whether or not the page is all_visible
	 * and all_frozen and use this information to update the VM. all_visible
	 * implies lpdead_items == 0, but don't trust all_frozen result unless
	 * all_visible is also set to true.
	 */
	presult->all_frozen = true;

	/* Scan the page */
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid;

		/* see preceding loop */
		if (off_loc)
			*off_loc = offnum;

		if (pagefrz)
			prune_prepare_freeze_tuple(page, offnum, &prstate,
									   pagefrz, frozen, presult);

		itemid = PageGetItemId(page, offnum);

		if (ItemIdIsNormal(itemid) &&
			prstate.htsv[offnum] != HEAPTUPLE_DEAD)
		{
			Assert(prstate.htsv[offnum] != -1);

			/*
			 * Deliberately don't set hastup for LP_DEAD items.  We make the
			 * soft assumption that any LP_DEAD items encountered here will
			 * become LP_UNUSED later on, before count_nondeletable_pages is
			 * reached.  If we don't make this assumption then rel truncation
			 * will only happen every other VACUUM, at most.  Besides, VACUUM
			 * must treat hastup/nonempty_pages as provisional no matter how
			 * LP_DEAD items are handled (handled here, or handled later on).
			 */
			presult->hastup = true;
		}

		/* Ignore items already processed as part of an earlier chain */
		if (prstate.marked[offnum])
			continue;

		/* Nothing to do if slot is empty */
		if (!ItemIdIsUsed(itemid))
			continue;

		/* Process this item or chain of items */
		presult->ndeleted += heap_prune_chain(buffer, offnum,
											  &prstate, presult);

	}

	/* Clear the offset information once we have processed the given page. */
	if (off_loc)
		*off_loc = InvalidOffsetNumber;

	do_prune = prstate.nredirected > 0 ||
		prstate.ndead > 0 ||
		prstate.nunused > 0;

	/*
	 * Only incur overhead of checking if we will do an FPI if we might use
	 * the information.
	 */
	if (do_prune && pagefrz)
		prune_fpi = XLogCheckBufferNeedsBackup(buffer);

	/* Is the whole page freezable? And is there something to freeze */
	whole_page_freezable = presult->all_visible_except_removable &&
		presult->all_frozen;

	/*
	 * Freeze the page when heap_prepare_freeze_tuple indicates that at least
	 * one XID/MXID from before FreezeLimit/MultiXactCutoff is present.  Also
	 * freeze when pruning generated an FPI, if doing so means that we set the
	 * page all-frozen afterwards (might not happen until final heap pass).
	 * XXX: Previously, we knew if pruning emitted an FPI by checking
	 * pgWalUsage.wal_fpi before and after pruning. Once the freeze and prune
	 * records are combined, this heuristic couldn't be used anymore. The
	 * opportunistic freeze heuristic must be improved; however, for now, try
	 * to approximate it.
	 */

	do_freeze = pagefrz &&
		(pagefrz->freeze_required ||
		 (whole_page_freezable && presult->nfrozen > 0 && (prune_fpi || hint_bit_fpi)));

	if (do_freeze)
	{
		heap_pre_freeze_checks(buffer, frozen, presult->nfrozen);
		frz_conflict_horizon = heap_frz_conflict_horizon(presult, pagefrz);
	}
	else if (!pagefrz || !presult->all_frozen || presult->nfrozen > 0)
	{
		/*
		 * If we will neither freeze tuples on the page nor set the page all
		 * frozen in the visibility map, the page is not all frozen and there
		 * will be no newly frozen tuples.
		 */
		presult->all_frozen = false;
		presult->nfrozen = 0;	/* avoid miscounts in instrumentation */
	}

	/* Record number of newly-set-LP_DEAD items for caller */
	presult->nnewlpdead = prstate.ndead;


	/* Have we found any prunable items? */
	if (!do_prune)
	{
		/* Any error while applying the changes is critical */
		START_CRIT_SECTION();

		/*
		 * If we didn't prune anything, but have found a new value for the
		 * pd_prune_xid field, update it and mark the buffer dirty. This is
		 * treated as a non-WAL-logged hint.
		 *
		 * Also clear the "page is full" flag if it is set, since there's no
		 * point in repeating the prune/defrag process until something else
		 * happens to the page.
		 */
		if (((PageHeader) page)->pd_prune_xid != prstate.new_prune_xid ||
			PageIsFull(page))
		{
			((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;
			PageClearFull(page);
			MarkBufferDirtyHint(buffer, true);
		}

		hint_bit_fpi = fpi_before != pgWalUsage.wal_fpi;

		/*
		 * We may have decided not to opportunistically freeze above because
		 * pruning would not emit an FPI. Now, however, if checksums are
		 * enabled, setting the hint bit may have emitted an FPI. Check again
		 * if we should freeze.
		 */
		if (!do_freeze && hint_bit_fpi)
			do_freeze = pagefrz &&
				(pagefrz->freeze_required ||
				 (whole_page_freezable && presult->nfrozen > 0));

		if (do_freeze)
		{
			heap_freeze_execute_prepared(relation, buffer,
										 frz_conflict_horizon,
										 frozen, presult->nfrozen);
		}
		else if (!pagefrz || !presult->all_frozen || presult->nfrozen > 0)
		{
			presult->all_frozen = false;
			presult->nfrozen = 0;
		}

		END_CRIT_SECTION();
		return;
	}

	START_CRIT_SECTION();

	/*
	 * Apply the planned item changes, then repair page fragmentation, and
	 * update the page's hint bit about whether it has free line pointers.
	 */
	heap_page_prune_execute(buffer,
							prstate.redirected, prstate.nredirected,
							prstate.nowdead, prstate.ndead,
							prstate.nowunused, prstate.nunused);

	/*
	 * Update the page's pd_prune_xid field to either zero, or the lowest XID
	 * of any soon-prunable tuple.
	 */
	((PageHeader) page)->pd_prune_xid = prstate.new_prune_xid;

	/*
	 * Also clear the "page is full" flag, since there's no point in repeating
	 * the prune/defrag process until something else happens to the page.
	 */
	PageClearFull(page);

	if (do_freeze)
		heap_freeze_prepared_tuples(buffer, frozen, presult->nfrozen);

	MarkBufferDirty(buffer);

	/*
	 * Emit a WAL XLOG_HEAP2_PRUNE record showing what we did
	 */
	if (RelationNeedsWAL(relation))
	{
		xl_heap_prune xlrec;
		XLogRecPtr	recptr;

		xl_heap_freeze_plan plans[MaxHeapTuplesPerPage];
		OffsetNumber offsets[MaxHeapTuplesPerPage];

		xlrec.isCatalogRel = RelationIsAccessibleInLogicalDecoding(relation);
		xlrec.nredirected = prstate.nredirected;
		xlrec.ndead = prstate.ndead;
		xlrec.nunused = prstate.nunused;
		xlrec.nplans = 0;

		/*
		 * The snapshotConflictHorizon for the whole record should be the most
		 * conservative of all the horizons calculated for any of the possible
		 * modifications. If this record will prune tuples, any transactions
		 * on the standby older than the youngest xmax of the most recently
		 * removed tuple this record will prune will conflict. If this record
		 * will freeze tuples, any transactions on the standby with xids older
		 * than the youngest tuple this record will freeze will conflict.
		 */
		if (do_freeze)
			xlrec.snapshotConflictHorizon = Max(prstate.snapshotConflictHorizon,
												frz_conflict_horizon);
		else
			xlrec.snapshotConflictHorizon = prstate.snapshotConflictHorizon;

		/*
		 * Prepare deduplicated representation for use in WAL record
		 * Destructively sorts tuples array in-place.
		 */
		if (do_freeze)
			xlrec.nplans = heap_log_freeze_plan(frozen,
												presult->nfrozen, plans, offsets);

		XLogBeginInsert();
		XLogRegisterData((char *) &xlrec, SizeOfHeapPrune);

		XLogRegisterBuffer(0, buffer, REGBUF_STANDARD);

		/*
		 * The OffsetNumber arrays are not actually in the buffer, but we
		 * pretend that they are.  When XLogInsert stores the whole buffer,
		 * the offset arrays need not be stored too.
		 */
		if (xlrec.nplans > 0)
			XLogRegisterBufData(0, (char *) plans,
								xlrec.nplans * sizeof(xl_heap_freeze_plan));

		if (prstate.nredirected > 0)
			XLogRegisterBufData(0, (char *) prstate.redirected,
								prstate.nredirected *
								sizeof(OffsetNumber) * 2);

		if (prstate.ndead > 0)
			XLogRegisterBufData(0, (char *) prstate.nowdead,
								prstate.ndead * sizeof(OffsetNumber));

		if (prstate.nunused > 0)
			XLogRegisterBufData(0, (char *) prstate.nowunused,
								prstate.nunused * sizeof(OffsetNumber));

		if (xlrec.nplans > 0)
			XLogRegisterBufData(0, (char *) offsets,
								presult->nfrozen * sizeof(OffsetNumber));

		recptr = XLogInsert(RM_HEAP2_ID, XLOG_HEAP2_PRUNE);

		PageSetLSN(BufferGetPage(buffer), recptr);
	}

	END_CRIT_SECTION();
}


/*
 * Perform visibility checks for heap pruning.
 */
static HTSV_Result
heap_prune_satisfies_vacuum(PruneState *prstate, HeapTuple tup, Buffer buffer)
{
	HTSV_Result res;
	TransactionId dead_after;

	res = HeapTupleSatisfiesVacuumHorizon(tup, buffer, &dead_after);

	if (res != HEAPTUPLE_RECENTLY_DEAD)
		return res;

	if (GlobalVisTestIsRemovableXid(prstate->vistest, dead_after))
		res = HEAPTUPLE_DEAD;

	return res;
}


/*
 * Pruning calculates tuple visibility once and saves the results in an array
 * of int8. See PruneState.htsv for details. This helper function is meant to
 * guard against examining visibility status array members which have not yet
 * been computed.
 */
static inline HTSV_Result
htsv_get_valid_status(int status)
{
	Assert(status >= HEAPTUPLE_DEAD &&
		   status <= HEAPTUPLE_DELETE_IN_PROGRESS);
	return (HTSV_Result) status;
}

/*
 * Prune specified line pointer or a HOT chain originating at line pointer.
 *
 * Tuple visibility information is provided in prstate->htsv.
 *
 * If the item is an index-referenced tuple (i.e. not a heap-only tuple),
 * the HOT chain is pruned by removing all DEAD tuples at the start of the HOT
 * chain.  We also prune any RECENTLY_DEAD tuples preceding a DEAD tuple.
 * This is OK because a RECENTLY_DEAD tuple preceding a DEAD tuple is really
 * DEAD, our visibility test is just too coarse to detect it.
 *
 * In general, pruning must never leave behind a DEAD tuple that still has
 * tuple storage.  VACUUM isn't prepared to deal with that case.  That's why
 * VACUUM prunes the same heap page a second time (without dropping its lock
 * in the interim) when it sees a newly DEAD tuple that we initially saw as
 * in-progress.  Retrying pruning like this can only happen when an inserting
 * transaction concurrently aborts.
 *
 * The root line pointer is redirected to the tuple immediately after the
 * latest DEAD tuple.  If all tuples in the chain are DEAD, the root line
 * pointer is marked LP_DEAD.  (This includes the case of a DEAD simple
 * tuple, which we treat as a chain of length 1.)
 *
 * We don't actually change the page here. We just add entries to the arrays in
 * prstate showing the changes to be made.  Items to be redirected are added
 * to the redirected[] array (two entries per redirection); items to be set to
 * LP_DEAD state are added to nowdead[]; and items to be set to LP_UNUSED
 * state are added to nowunused[].
 *
 * Returns the number of tuples (to be) deleted from the page.
 */
static int
heap_prune_chain(Buffer buffer, OffsetNumber rootoffnum,
				 PruneState *prstate, PruneResult *presult)
{
	int			ndeleted = 0;
	Page		dp = (Page) BufferGetPage(buffer);
	TransactionId priorXmax = InvalidTransactionId;
	ItemId		rootlp;
	HeapTupleHeader htup;
	OffsetNumber latestdead = InvalidOffsetNumber,
				maxoff = PageGetMaxOffsetNumber(dp),
				offnum;
	OffsetNumber chainitems[MaxHeapTuplesPerPage];
	int			nchain = 0,
				i;

	rootlp = PageGetItemId(dp, rootoffnum);

	/*
	 * If it's a heap-only tuple, then it is not the start of a HOT chain.
	 */
	if (ItemIdIsNormal(rootlp))
	{
		Assert(prstate->htsv[rootoffnum] != -1);
		htup = (HeapTupleHeader) PageGetItem(dp, rootlp);

		if (HeapTupleHeaderIsHeapOnly(htup))
		{
			/*
			 * If the tuple is DEAD and doesn't chain to anything else, mark
			 * it unused immediately.  (If it does chain, we can only remove
			 * it as part of pruning its chain.)
			 *
			 * We need this primarily to handle aborted HOT updates, that is,
			 * XMIN_INVALID heap-only tuples.  Those might not be linked to by
			 * any chain, since the parent tuple might be re-updated before
			 * any pruning occurs.  So we have to be able to reap them
			 * separately from chain-pruning.  (Note that
			 * HeapTupleHeaderIsHotUpdated will never return true for an
			 * XMIN_INVALID tuple, so this code will work even when there were
			 * sequential updates within the aborted transaction.)
			 *
			 * Note that we might first arrive at a dead heap-only tuple
			 * either here or while following a chain below.  Whichever path
			 * gets there first will mark the tuple unused.
			 */
			if (prstate->htsv[rootoffnum] == HEAPTUPLE_DEAD &&
				!HeapTupleHeaderIsHotUpdated(htup))
			{
				heap_prune_record_unused(prstate, rootoffnum);
				HeapTupleHeaderAdvanceConflictHorizon(htup,
													  &prstate->snapshotConflictHorizon);
				ndeleted++;
			}

			/* Nothing more to do */
			return ndeleted;
		}
	}

	/* Start from the root tuple */
	offnum = rootoffnum;

	/* while not end of the chain */
	for (;;)
	{
		ItemId		lp;
		bool		tupdead,
					recent_dead;

		/* Sanity check (pure paranoia) */
		if (offnum < FirstOffsetNumber)
			break;

		/*
		 * An offset past the end of page's line pointer array is possible
		 * when the array was truncated (original item must have been unused)
		 */
		if (offnum > maxoff)
			break;

		/* If item is already processed, stop --- it must not be same chain */
		if (prstate->marked[offnum])
			break;

		lp = PageGetItemId(dp, offnum);

		/* Unused item obviously isn't part of the chain */
		if (!ItemIdIsUsed(lp))
			break;

		/*
		 * If we are looking at the redirected root line pointer, jump to the
		 * first normal tuple in the chain.  If we find a redirect somewhere
		 * else, stop --- it must not be same chain.
		 */
		if (ItemIdIsRedirected(lp))
		{
			if (nchain > 0)
				break;			/* not at start of chain */
			chainitems[nchain++] = offnum;
			offnum = ItemIdGetRedirect(rootlp);
			continue;
		}

		/*
		 * Likewise, a dead line pointer can't be part of the chain. (We
		 * already eliminated the case of dead root tuple outside this
		 * function.)
		 */
		if (ItemIdIsDead(lp))
		{
			/*
			 * If the caller set mark_unused_now true, we can set dead line
			 * pointers LP_UNUSED now. We don't increment ndeleted here since
			 * the LP was already marked dead. If it will not be marked
			 * LP_UNUSED, it will remain LP_DEAD, making the page not
			 * all_visible.
			 */
			if (unlikely(prstate->mark_unused_now))
				heap_prune_record_unused(prstate, offnum);
			else
				presult->all_visible = false;

			break;
		}

		Assert(ItemIdIsNormal(lp));
		htup = (HeapTupleHeader) PageGetItem(dp, lp);

		/*
		 * Check the tuple XMIN against prior XMAX, if any
		 */
		if (TransactionIdIsValid(priorXmax) &&
			!TransactionIdEquals(HeapTupleHeaderGetXmin(htup), priorXmax))
			break;

		/*
		 * OK, this tuple is indeed a member of the chain.
		 */
		chainitems[nchain++] = offnum;

		/*
		 * Check tuple's visibility status.
		 */
		tupdead = recent_dead = false;

		switch (htsv_get_valid_status(prstate->htsv[offnum]))
		{
			case HEAPTUPLE_DEAD:
				tupdead = true;
				break;

			case HEAPTUPLE_RECENTLY_DEAD:
				recent_dead = true;

				/*
				 * This tuple may soon become DEAD.  Update the hint field so
				 * that the page is reconsidered for pruning in future.
				 */
				heap_prune_record_prunable(prstate,
										   HeapTupleHeaderGetUpdateXid(htup));
				break;

			case HEAPTUPLE_DELETE_IN_PROGRESS:

				/*
				 * This tuple may soon become DEAD.  Update the hint field so
				 * that the page is reconsidered for pruning in future.
				 */
				heap_prune_record_prunable(prstate,
										   HeapTupleHeaderGetUpdateXid(htup));
				break;

			case HEAPTUPLE_LIVE:
			case HEAPTUPLE_INSERT_IN_PROGRESS:

				/*
				 * If we wanted to optimize for aborts, we might consider
				 * marking the page prunable when we see INSERT_IN_PROGRESS.
				 * But we don't.  See related decisions about when to mark the
				 * page prunable in heapam.c.
				 */
				break;

			default:
				elog(ERROR, "unexpected HeapTupleSatisfiesVacuum result");
				break;
		}

		/*
		 * Remember the last DEAD tuple seen.  We will advance past
		 * RECENTLY_DEAD tuples just in case there's a DEAD one after them;
		 * but we can't advance past anything else.  We have to make sure that
		 * we don't miss any DEAD tuples, since DEAD tuples that still have
		 * tuple storage after pruning will confuse VACUUM.
		 */
		if (tupdead)
		{
			latestdead = offnum;
			HeapTupleHeaderAdvanceConflictHorizon(htup,
												  &prstate->snapshotConflictHorizon);
		}
		else if (!recent_dead)
			break;

		/*
		 * If the tuple is not HOT-updated, then we are at the end of this
		 * HOT-update chain.
		 */
		if (!HeapTupleHeaderIsHotUpdated(htup))
			break;

		/* HOT implies it can't have moved to different partition */
		Assert(!HeapTupleHeaderIndicatesMovedPartitions(htup));

		/*
		 * Advance to next chain member.
		 */
		Assert(ItemPointerGetBlockNumber(&htup->t_ctid) ==
			   BufferGetBlockNumber(buffer));
		offnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
		priorXmax = HeapTupleHeaderGetUpdateXid(htup);
	}

	/*
	 * If we found a DEAD tuple in the chain, adjust the HOT chain so that all
	 * the DEAD tuples at the start of the chain are removed and the root line
	 * pointer is appropriately redirected.
	 */
	if (OffsetNumberIsValid(latestdead))
	{
		/*
		 * Mark as unused each intermediate item that we are able to remove
		 * from the chain.
		 *
		 * When the previous item is the last dead tuple seen, we are at the
		 * right candidate for redirection.
		 */
		for (i = 1; (i < nchain) && (chainitems[i - 1] != latestdead); i++)
		{
			heap_prune_record_unused(prstate, chainitems[i]);
			ndeleted++;
		}

		/*
		 * If the root entry had been a normal tuple, we are deleting it, so
		 * count it in the result.  But changing a redirect (even to DEAD
		 * state) doesn't count.
		 */
		if (ItemIdIsNormal(rootlp))
			ndeleted++;

		/*
		 * If the DEAD tuple is at the end of the chain, the entire chain is
		 * dead and the root line pointer can be marked dead.  Otherwise just
		 * redirect the root to the correct chain member.
		 */
		if (i >= nchain)
			heap_prune_record_dead_or_unused(prstate, rootoffnum, presult);
		else
			heap_prune_record_redirect(prstate, rootoffnum, chainitems[i], presult);
	}
	else if (nchain < 2 && ItemIdIsRedirected(rootlp))
	{
		/*
		 * We found a redirect item that doesn't point to a valid follow-on
		 * item.  This can happen if the loop in heap_page_prune caused us to
		 * visit the dead successor of a redirect item before visiting the
		 * redirect item.  We can clean up by setting the redirect item to
		 * DEAD state or LP_UNUSED if the caller indicated.
		 */
		heap_prune_record_dead_or_unused(prstate, rootoffnum, presult);
	}

	return ndeleted;
}

/*
 * While pruning, before actually executing pruning and updating the line
 * pointers, we may consider freezing tuples referred to by LP_NORMAL line
 * pointers whose visibility status is not HEAPTUPLE_DEAD. That is to say, we
 * want to consider freezing normal tuples which will not be removed.
*/
static void
prune_prepare_freeze_tuple(Page page, OffsetNumber offnum, PruneState *prstate,
						   HeapPageFreeze *pagefrz,
						   HeapTupleFreeze *frozen,
						   PruneResult *presult)
{
	bool		totally_frozen;
	HeapTupleHeader htup;
	ItemId		itemid;

	Assert(pagefrz);

	itemid = PageGetItemId(page, offnum);

	if (!ItemIdIsNormal(itemid))
		return;

	/* We do not consider freezing tuples which will be removed. */
	if (prstate->htsv[offnum] == HEAPTUPLE_DEAD ||
		prstate->htsv[offnum] == -1)
		return;

	htup = (HeapTupleHeader) PageGetItem(page, itemid);

	/* Tuple with storage -- consider need to freeze */
	if ((heap_prepare_freeze_tuple(htup, pagefrz,
								   &frozen[presult->nfrozen],
								   &totally_frozen)))
	{
		/* Save prepared freeze plan for later */
		frozen[presult->nfrozen++].offset = offnum;
	}

	/*
	 * If any tuple isn't either totally frozen already or eligible to become
	 * totally frozen (according to its freeze plan), then the page definitely
	 * cannot be set all-frozen in the visibility map later on
	 */
	if (!totally_frozen)
		presult->all_frozen = false;
}

/* Record lowest soon-prunable XID */
static void
heap_prune_record_prunable(PruneState *prstate, TransactionId xid)
{
	/*
	 * This should exactly match the PageSetPrunable macro.  We can't store
	 * directly into the page header yet, so we update working state.
	 */
	Assert(TransactionIdIsNormal(xid));
	if (!TransactionIdIsValid(prstate->new_prune_xid) ||
		TransactionIdPrecedes(xid, prstate->new_prune_xid))
		prstate->new_prune_xid = xid;
}

/* Record line pointer to be redirected */
static void
heap_prune_record_redirect(PruneState *prstate,
						   OffsetNumber offnum, OffsetNumber rdoffnum,
						   PruneResult *presult)
{
	Assert(prstate->nredirected < MaxHeapTuplesPerPage);
	prstate->redirected[prstate->nredirected * 2] = offnum;
	prstate->redirected[prstate->nredirected * 2 + 1] = rdoffnum;
	prstate->nredirected++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;
	Assert(!prstate->marked[rdoffnum]);
	prstate->marked[rdoffnum] = true;

	presult->hastup = true;
}

/* Record line pointer to be marked dead */
static void
heap_prune_record_dead(PruneState *prstate, OffsetNumber offnum,
					   PruneResult *presult)
{
	Assert(prstate->ndead < MaxHeapTuplesPerPage);
	prstate->nowdead[prstate->ndead] = offnum;
	prstate->ndead++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;

	/*
	 * Setting the line pointer LP_DEAD means the page will definitely not be
	 * all_visible.
	 */
	presult->all_visible = false;
}

/*
 * Depending on whether or not the caller set mark_unused_now to true, record that a
 * line pointer should be marked LP_DEAD or LP_UNUSED. There are other cases in
 * which we will mark line pointers LP_UNUSED, but we will not mark line
 * pointers LP_DEAD if mark_unused_now is true.
 */
static void
heap_prune_record_dead_or_unused(PruneState *prstate, OffsetNumber offnum,
								 PruneResult *presult)
{
	/*
	 * If the caller set mark_unused_now to true, we can remove dead tuples
	 * during pruning instead of marking their line pointers dead. Set this
	 * tuple's line pointer LP_UNUSED. We hint that this option is less
	 * likely.
	 */
	if (unlikely(prstate->mark_unused_now))
		heap_prune_record_unused(prstate, offnum);
	else
		heap_prune_record_dead(prstate, offnum, presult);
}

/* Record line pointer to be marked unused */
static void
heap_prune_record_unused(PruneState *prstate, OffsetNumber offnum)
{
	Assert(prstate->nunused < MaxHeapTuplesPerPage);
	prstate->nowunused[prstate->nunused] = offnum;
	prstate->nunused++;
	Assert(!prstate->marked[offnum]);
	prstate->marked[offnum] = true;
}


/*
 * Perform the actual page changes needed by heap_page_prune.
 * It is expected that the caller has a full cleanup lock on the
 * buffer.
 */
void
heap_page_prune_execute(Buffer buffer,
						OffsetNumber *redirected, int nredirected,
						OffsetNumber *nowdead, int ndead,
						OffsetNumber *nowunused, int nunused)
{
	Page		page = (Page) BufferGetPage(buffer);
	OffsetNumber *offnum;
	HeapTupleHeader htup PG_USED_FOR_ASSERTS_ONLY;

	/* Shouldn't be called unless there's something to do */
	Assert(nredirected > 0 || ndead > 0 || nunused > 0);

	/* Update all redirected line pointers */
	offnum = redirected;
	for (int i = 0; i < nredirected; i++)
	{
		OffsetNumber fromoff = *offnum++;
		OffsetNumber tooff = *offnum++;
		ItemId		fromlp = PageGetItemId(page, fromoff);
		ItemId		tolp PG_USED_FOR_ASSERTS_ONLY;

#ifdef USE_ASSERT_CHECKING

		/*
		 * Any existing item that we set as an LP_REDIRECT (any 'from' item)
		 * must be the first item from a HOT chain.  If the item has tuple
		 * storage then it can't be a heap-only tuple.  Otherwise we are just
		 * maintaining an existing LP_REDIRECT from an existing HOT chain that
		 * has been pruned at least once before now.
		 */
		if (!ItemIdIsRedirected(fromlp))
		{
			Assert(ItemIdHasStorage(fromlp) && ItemIdIsNormal(fromlp));

			htup = (HeapTupleHeader) PageGetItem(page, fromlp);
			Assert(!HeapTupleHeaderIsHeapOnly(htup));
		}
		else
		{
			/* We shouldn't need to redundantly set the redirect */
			Assert(ItemIdGetRedirect(fromlp) != tooff);
		}

		/*
		 * The item that we're about to set as an LP_REDIRECT (the 'from'
		 * item) will point to an existing item (the 'to' item) that is
		 * already a heap-only tuple.  There can be at most one LP_REDIRECT
		 * item per HOT chain.
		 *
		 * We need to keep around an LP_REDIRECT item (after original
		 * non-heap-only root tuple gets pruned away) so that it's always
		 * possible for VACUUM to easily figure out what TID to delete from
		 * indexes when an entire HOT chain becomes dead.  A heap-only tuple
		 * can never become LP_DEAD; an LP_REDIRECT item or a regular heap
		 * tuple can.
		 *
		 * This check may miss problems, e.g. the target of a redirect could
		 * be marked as unused subsequently. The page_verify_redirects() check
		 * below will catch such problems.
		 */
		tolp = PageGetItemId(page, tooff);
		Assert(ItemIdHasStorage(tolp) && ItemIdIsNormal(tolp));
		htup = (HeapTupleHeader) PageGetItem(page, tolp);
		Assert(HeapTupleHeaderIsHeapOnly(htup));
#endif

		ItemIdSetRedirect(fromlp, tooff);
	}

	/* Update all now-dead line pointers */
	offnum = nowdead;
	for (int i = 0; i < ndead; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

#ifdef USE_ASSERT_CHECKING

		/*
		 * An LP_DEAD line pointer must be left behind when the original item
		 * (which is dead to everybody) could still be referenced by a TID in
		 * an index.  This should never be necessary with any individual
		 * heap-only tuple item, though. (It's not clear how much of a problem
		 * that would be, but there is no reason to allow it.)
		 */
		if (ItemIdHasStorage(lp))
		{
			Assert(ItemIdIsNormal(lp));
			htup = (HeapTupleHeader) PageGetItem(page, lp);
			Assert(!HeapTupleHeaderIsHeapOnly(htup));
		}
		else
		{
			/* Whole HOT chain becomes dead */
			Assert(ItemIdIsRedirected(lp));
		}
#endif

		ItemIdSetDead(lp);
	}

	/* Update all now-unused line pointers */
	offnum = nowunused;
	for (int i = 0; i < nunused; i++)
	{
		OffsetNumber off = *offnum++;
		ItemId		lp = PageGetItemId(page, off);

#ifdef USE_ASSERT_CHECKING

		/*
		 * When heap_page_prune() was called, mark_unused_now may have been
		 * passed as true, which allows would-be LP_DEAD items to be made
		 * LP_UNUSED instead. This is only possible if the relation has no
		 * indexes. If there are any dead items, then mark_unused_now was not
		 * true and every item being marked LP_UNUSED must refer to a
		 * heap-only tuple.
		 */
		if (ndead > 0)
		{
			Assert(ItemIdHasStorage(lp) && ItemIdIsNormal(lp));
			htup = (HeapTupleHeader) PageGetItem(page, lp);
			Assert(HeapTupleHeaderIsHeapOnly(htup));
		}
		else
		{
			Assert(ItemIdIsUsed(lp));
		}

#endif

		ItemIdSetUnused(lp);
	}

	/*
	 * Finally, repair any fragmentation, and update the page's hint bit about
	 * whether it has free pointers.
	 */
	PageRepairFragmentation(page);

	/*
	 * Now that the page has been modified, assert that redirect items still
	 * point to valid targets.
	 */
	page_verify_redirects(page);
}


/*
 * If built with assertions, verify that all LP_REDIRECT items point to a
 * valid item.
 *
 * One way that bugs related to HOT pruning show is redirect items pointing to
 * removed tuples. It's not trivial to reliably check that marking an item
 * unused will not orphan a redirect item during heap_prune_chain() /
 * heap_page_prune_execute(), so we additionally check the whole page after
 * pruning. Without this check such bugs would typically only cause asserts
 * later, potentially well after the corruption has been introduced.
 *
 * Also check comments in heap_page_prune_execute()'s redirection loop.
 */
static void
page_verify_redirects(Page page)
{
#ifdef USE_ASSERT_CHECKING
	OffsetNumber offnum;
	OffsetNumber maxoff;

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber;
		 offnum <= maxoff;
		 offnum = OffsetNumberNext(offnum))
	{
		ItemId		itemid = PageGetItemId(page, offnum);
		OffsetNumber targoff;
		ItemId		targitem;
		HeapTupleHeader htup;

		if (!ItemIdIsRedirected(itemid))
			continue;

		targoff = ItemIdGetRedirect(itemid);
		targitem = PageGetItemId(page, targoff);

		Assert(ItemIdIsUsed(targitem));
		Assert(ItemIdIsNormal(targitem));
		Assert(ItemIdHasStorage(targitem));
		htup = (HeapTupleHeader) PageGetItem(page, targitem);
		Assert(HeapTupleHeaderIsHeapOnly(htup));
	}
#endif
}


/*
 * For all items in this page, find their respective root line pointers.
 * If item k is part of a HOT-chain with root at item j, then we set
 * root_offsets[k - 1] = j.
 *
 * The passed-in root_offsets array must have MaxHeapTuplesPerPage entries.
 * Unused entries are filled with InvalidOffsetNumber (zero).
 *
 * The function must be called with at least share lock on the buffer, to
 * prevent concurrent prune operations.
 *
 * Note: The information collected here is valid only as long as the caller
 * holds a pin on the buffer. Once pin is released, a tuple might be pruned
 * and reused by a completely unrelated tuple.
 */
void
heap_get_root_tuples(Page page, OffsetNumber *root_offsets)
{
	OffsetNumber offnum,
				maxoff;

	MemSet(root_offsets, InvalidOffsetNumber,
		   MaxHeapTuplesPerPage * sizeof(OffsetNumber));

	maxoff = PageGetMaxOffsetNumber(page);
	for (offnum = FirstOffsetNumber; offnum <= maxoff; offnum = OffsetNumberNext(offnum))
	{
		ItemId		lp = PageGetItemId(page, offnum);
		HeapTupleHeader htup;
		OffsetNumber nextoffnum;
		TransactionId priorXmax;

		/* skip unused and dead items */
		if (!ItemIdIsUsed(lp) || ItemIdIsDead(lp))
			continue;

		if (ItemIdIsNormal(lp))
		{
			htup = (HeapTupleHeader) PageGetItem(page, lp);

			/*
			 * Check if this tuple is part of a HOT-chain rooted at some other
			 * tuple. If so, skip it for now; we'll process it when we find
			 * its root.
			 */
			if (HeapTupleHeaderIsHeapOnly(htup))
				continue;

			/*
			 * This is either a plain tuple or the root of a HOT-chain.
			 * Remember it in the mapping.
			 */
			root_offsets[offnum - 1] = offnum;

			/* If it's not the start of a HOT-chain, we're done with it */
			if (!HeapTupleHeaderIsHotUpdated(htup))
				continue;

			/* Set up to scan the HOT-chain */
			nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
			priorXmax = HeapTupleHeaderGetUpdateXid(htup);
		}
		else
		{
			/* Must be a redirect item. We do not set its root_offsets entry */
			Assert(ItemIdIsRedirected(lp));
			/* Set up to scan the HOT-chain */
			nextoffnum = ItemIdGetRedirect(lp);
			priorXmax = InvalidTransactionId;
		}

		/*
		 * Now follow the HOT-chain and collect other tuples in the chain.
		 *
		 * Note: Even though this is a nested loop, the complexity of the
		 * function is O(N) because a tuple in the page should be visited not
		 * more than twice, once in the outer loop and once in HOT-chain
		 * chases.
		 */
		for (;;)
		{
			/* Sanity check (pure paranoia) */
			if (offnum < FirstOffsetNumber)
				break;

			/*
			 * An offset past the end of page's line pointer array is possible
			 * when the array was truncated
			 */
			if (offnum > maxoff)
				break;

			lp = PageGetItemId(page, nextoffnum);

			/* Check for broken chains */
			if (!ItemIdIsNormal(lp))
				break;

			htup = (HeapTupleHeader) PageGetItem(page, lp);

			if (TransactionIdIsValid(priorXmax) &&
				!TransactionIdEquals(priorXmax, HeapTupleHeaderGetXmin(htup)))
				break;

			/* Remember the root line pointer for this item */
			root_offsets[nextoffnum - 1] = offnum;

			/* Advance to next chain member, if any */
			if (!HeapTupleHeaderIsHotUpdated(htup))
				break;

			/* HOT implies it can't have moved to different partition */
			Assert(!HeapTupleHeaderIndicatesMovedPartitions(htup));

			nextoffnum = ItemPointerGetOffsetNumber(&htup->t_ctid);
			priorXmax = HeapTupleHeaderGetUpdateXid(htup);
		}
	}
}
